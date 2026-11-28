//go:build oversync_audit

package oversync

import (
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type auditTCPProxyPair struct {
	downstream net.Conn
	upstream   net.Conn
	closeOnce  sync.Once
}

func (p *auditTCPProxyPair) close() {
	p.closeOnce.Do(func() {
		_ = p.downstream.Close()
		_ = p.upstream.Close()
	})
}

type auditTCPProxy struct {
	listener  net.Listener
	target    string
	dialer    net.Dialer
	mu        sync.Mutex
	closed    bool
	pairs     map[*auditTCPProxyPair]struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func newAuditTCPProxy(listenAddress, targetAddress string) (*auditTCPProxy, error) {
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return nil, err
	}
	proxy := &auditTCPProxy{
		listener: listener,
		target:   targetAddress,
		dialer:   net.Dialer{Timeout: 2 * time.Second},
		pairs:    make(map[*auditTCPProxyPair]struct{}),
	}
	if proof, ok := auditSchemaGenerationProofByAddress.Load(targetAddress); ok {
		auditSchemaGenerationProofByAddress.Store(listener.Addr().String(), proof)
	}
	proxy.wg.Add(1)
	go proxy.accept()
	return proxy, nil
}

func (p *auditTCPProxy) Address() string {
	return p.listener.Addr().String()
}

func (p *auditTCPProxy) ActiveConnections() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pairs)
}

func (p *auditTCPProxy) accept() {
	defer p.wg.Done()
	for {
		downstream, err := p.listener.Accept()
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if closed {
				return
			}
			continue
		}
		p.wg.Add(1)
		go p.connect(downstream)
	}
}

func (p *auditTCPProxy) connect(downstream net.Conn) {
	defer p.wg.Done()
	upstream, err := p.dialer.Dial("tcp", p.target)
	if err != nil {
		_ = downstream.Close()
		return
	}
	pair := &auditTCPProxyPair{downstream: downstream, upstream: upstream}
	if !p.register(pair) {
		pair.close()
		return
	}
	defer p.unregister(pair)

	copyDone := make(chan struct{}, 2)
	copyOneWay := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		copyDone <- struct{}{}
	}
	go copyOneWay(upstream, downstream)
	go copyOneWay(downstream, upstream)
	<-copyDone
	pair.close()
	<-copyDone
}

func (p *auditTCPProxy) register(pair *auditTCPProxyPair) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.pairs[pair] = struct{}{}
	return true
}

func (p *auditTCPProxy) unregister(pair *auditTCPProxyPair) {
	pair.close()
	p.mu.Lock()
	delete(p.pairs, pair)
	p.mu.Unlock()
}

func (p *auditTCPProxy) Interrupt() int {
	p.mu.Lock()
	pairs := make([]*auditTCPProxyPair, 0, len(p.pairs))
	for pair := range p.pairs {
		pairs = append(pairs, pair)
	}
	p.mu.Unlock()
	for _, pair := range pairs {
		pair.close()
	}
	return len(pairs)
}

func (p *auditTCPProxy) Close() error {
	var closeErr error
	p.closeOnce.Do(func() {
		auditSchemaGenerationProofByAddress.Delete(p.listener.Addr().String())
		p.mu.Lock()
		p.closed = true
		pairs := make([]*auditTCPProxyPair, 0, len(p.pairs))
		for pair := range p.pairs {
			pairs = append(pairs, pair)
		}
		p.mu.Unlock()

		closeErr = p.listener.Close()
		for _, pair := range pairs {
			pair.close()
		}
		p.wg.Wait()
	})
	return closeErr
}

func startAuditTCPEchoServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	acceptDone := make(chan struct{})
	var connections sync.WaitGroup
	go func() {
		defer close(acceptDone)
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer connection.Close()
				_, _ = io.Copy(connection, connection)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		connections.Wait()
	})
	return listener.Addr().String()
}

func auditEchoRoundTrip(t *testing.T, connection net.Conn, payload []byte) {
	t.Helper()
	require.NoError(t, connection.SetDeadline(time.Now().Add(2*time.Second)))
	_, err := connection.Write(payload)
	require.NoError(t, err)
	received := make([]byte, len(payload))
	_, err = io.ReadFull(connection, received)
	require.NoError(t, err)
	require.Equal(t, payload, received)
}

func TestAuditTCPProxy_ForwardsHTTPToHelperAndClosesCleanly(t *testing.T) {
	databaseURL, schemaName := newAuditHelperDatabase(t)
	process := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	proxy, err := newAuditTCPProxy("127.0.0.1:0", process.address)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	client := newAuditHTTPClient()
	t.Cleanup(client.CloseIdleConnections)
	response, err := client.Get("http://" + proxy.Address() + "/syncx/health")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Eventually(t, func() bool {
		return proxy.ActiveConnections() == 0
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, proxy.Close())
	response, err = client.Get(process.URL() + "/syncx/health")
	require.NoError(t, err, "closing the proxy must not stop its target")
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
}

func TestAuditTCPProxy_InterruptsActiveConnectionsAndRecovers(t *testing.T) {
	targetAddress := startAuditTCPEchoServer(t)
	proxy, err := newAuditTCPProxy("127.0.0.1:0", targetAddress)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	first, err := net.DialTimeout("tcp", proxy.Address(), 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	auditEchoRoundTrip(t, first, []byte("before-interrupt"))
	require.Eventually(t, func() bool {
		return proxy.ActiveConnections() == 1
	}, 2*time.Second, 10*time.Millisecond)

	require.Equal(t, 1, proxy.Interrupt())
	require.NoError(t, first.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = first.Read(make([]byte, 1))
	require.Error(t, err, "interrupted connection must become unusable")
	require.Eventually(t, func() bool {
		return proxy.ActiveConnections() == 0
	}, 2*time.Second, 10*time.Millisecond)

	second, err := net.DialTimeout("tcp", proxy.Address(), 2*time.Second)
	require.NoError(t, err)
	auditEchoRoundTrip(t, second, []byte("after-interrupt"))
	require.NoError(t, second.Close())
	require.Eventually(t, func() bool {
		return proxy.ActiveConnections() == 0
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, proxy.Close())
	_, err = net.DialTimeout("tcp", proxy.Address(), 100*time.Millisecond)
	require.Error(t, err, "closed proxy listener must reject new connections")
}
