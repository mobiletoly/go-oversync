package oversync

import (
	"context"
	"testing"
	"time"
)

func mustReceiveBundleChangeEvent(t *testing.T, ch <-chan BundleChangeEvent) BundleChangeEvent {
	t.Helper()
	select {
	case event, ok := <-ch:
		if !ok {
			t.Fatal("expected event channel to remain open")
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bundle change event")
		return BundleChangeEvent{}
	}
}

func assertNoBundleChangeEvent(t *testing.T, ch <-chan BundleChangeEvent) {
	t.Helper()
	select {
	case event, ok := <-ch:
		t.Fatalf("unexpected bundle change event ok=%t event=%#v", ok, event)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitForBundleChangeSubscriberCount(t *testing.T, hub *bundleChangeHub, userPK int64, want int) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if got := hub.subscriberCount(userPK); got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("subscriber count did not reach %d, got %d", want, hub.subscriberCount(userPK))
		case <-ticker.C:
		}
	}
}

func TestBundleChangeHub_SubscribeAndUnsubscribe(t *testing.T) {
	hub := newBundleChangeHub()
	ctx, cancel := context.WithCancel(context.Background())
	ch, unsubscribe := hub.subscribe(ctx, 42, 0)

	if hub.subscriberCount(42) != 1 {
		t.Fatalf("expected one subscriber, got %d", hub.subscriberCount(42))
	}
	unsubscribe()
	if hub.subscriberCount(42) != 0 {
		t.Fatalf("expected subscriber removal, got %d", hub.subscriberCount(42))
	}
	if _, ok := <-ch; ok {
		t.Fatal("expected subscriber channel to close after unsubscribe")
	}
	cancel()
}

func TestBundleChangeHub_FanoutByUserPK(t *testing.T) {
	hub := newBundleChangeHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chA, _ := hub.subscribe(ctx, 42, 0)
	chB, _ := hub.subscribe(ctx, 42, 0)

	hub.publish(42, BundleChangeEvent{BundleSeq: 1})

	if event := mustReceiveBundleChangeEvent(t, chA); event.BundleSeq != 1 {
		t.Fatalf("unexpected first subscriber event: %#v", event)
	}
	if event := mustReceiveBundleChangeEvent(t, chB); event.BundleSeq != 1 {
		t.Fatalf("unexpected second subscriber event: %#v", event)
	}
}

func TestBundleChangeHub_IsolatesDifferentUserPKs(t *testing.T) {
	hub := newBundleChangeHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch42, _ := hub.subscribe(ctx, 42, 0)
	ch99, _ := hub.subscribe(ctx, 99, 0)

	hub.publish(42, BundleChangeEvent{BundleSeq: 1})

	if event := mustReceiveBundleChangeEvent(t, ch42); event.BundleSeq != 1 {
		t.Fatalf("unexpected event: %#v", event)
	}
	assertNoBundleChangeEvent(t, ch99)
}

func TestBundleChangeHub_CoalescesSlowSubscriber(t *testing.T) {
	hub := newBundleChangeHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := hub.subscribe(ctx, 42, 0)

	hub.publish(42, BundleChangeEvent{BundleSeq: 1})
	hub.publish(42, BundleChangeEvent{BundleSeq: 2})
	hub.publish(42, BundleChangeEvent{BundleSeq: 3})

	if event := mustReceiveBundleChangeEvent(t, ch); event.BundleSeq != 3 {
		t.Fatalf("expected coalesced latest bundle seq 3, got %#v", event)
	}
	assertNoBundleChangeEvent(t, ch)
}

func TestBundleChangeHub_DropsStaleEvent(t *testing.T) {
	hub := newBundleChangeHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := hub.subscribe(ctx, 42, 0)

	hub.publish(42, BundleChangeEvent{BundleSeq: 2})
	if event := mustReceiveBundleChangeEvent(t, ch); event.BundleSeq != 2 {
		t.Fatalf("unexpected event: %#v", event)
	}
	hub.publish(42, BundleChangeEvent{BundleSeq: 1})
	assertNoBundleChangeEvent(t, ch)
}

func TestBundleChangeHub_DoesNotBlockOnSlowSubscriber(t *testing.T) {
	hub := newBundleChangeHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := hub.subscribe(ctx, 42, 0)
	hub.publish(42, BundleChangeEvent{BundleSeq: 1})

	done := make(chan struct{})
	go func() {
		hub.publish(42, BundleChangeEvent{BundleSeq: 2})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publish blocked on full subscriber channel")
	}
	if event := mustReceiveBundleChangeEvent(t, ch); event.BundleSeq != 2 {
		t.Fatalf("expected latest queued event, got %#v", event)
	}
}

func TestBundleChangeHub_RemovesSubscriberOnContextCancel(t *testing.T) {
	hub := newBundleChangeHub()
	ctx, cancel := context.WithCancel(context.Background())
	ch, _ := hub.subscribe(ctx, 42, 0)

	cancel()
	waitForBundleChangeSubscriberCount(t, hub, 42, 0)
	if _, ok := <-ch; ok {
		t.Fatal("expected channel to close after context cancellation")
	}
}
