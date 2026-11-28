// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import "unsafe"

// pullPageAssembly builds one pull page while keeping the exact decoded size
// of its response and the allocations it retains as running totals. Admitting
// a bundle therefore costs work proportional to that bundle, not to the page
// built so far.
type pullPageAssembly struct {
	generation      SchemaGenerationCapabilities
	bundles         []Bundle
	responseBytes   int64
	admittedBuffers int64
	admittedBytes   int64
}

// newPullPageAssembly starts an empty page. capacity is the largest number of
// bundles the page may admit.
func newPullPageAssembly(generation SchemaGenerationCapabilities, stableBundleSeq int64, hasMore bool, capacity int) (*pullPageAssembly, error) {
	emptyResponseBytes, err := compactPullResponseDecodedBytes(generation, stableBundleSeq, nil, hasMore)
	if err != nil {
		return nil, err
	}
	return &pullPageAssembly{
		generation:    generation,
		bundles:       make([]Bundle, 0, capacity),
		responseBytes: emptyResponseBytes,
	}, nil
}

// responseBytesWith returns the exact decoded size the response would have if
// bundle were admitted next. The page is not changed.
func (p *pullPageAssembly) responseBytesWith(bundle *Bundle) (int64, error) {
	bundleBytes, err := compactPullBundleDecodedBytes(p.generation, bundle)
	if err != nil {
		return 0, err
	}
	if len(p.bundles) > 0 {
		bundleBytes++ // the separator before every element but the first
	}
	return p.responseBytes + bundleBytes, nil
}

// admit appends bundle to the page. responseBytes is the value
// responseBytesWith returned for it.
func (p *pullPageAssembly) admit(bundle *Bundle, responseBytes int64) {
	buffers, bytes := retainedBundleAllocations(bundle)
	p.admittedBuffers += buffers
	p.admittedBytes += bytes
	p.bundles = append(p.bundles, *bundle)
	p.responseBytes = responseBytes
}

// retainedWith reports the buffers and bytes owned by the page together with
// the bundles currently under examination.
func (p *pullPageAssembly) retainedWith(current ...*Bundle) (buffers, bytes int64) {
	buffers, bytes = retainedBundleAllocations(current...)
	buffers += 1 + p.admittedBuffers
	bytes += int64(cap(p.bundles))*int64(unsafe.Sizeof(Bundle{})) + p.admittedBytes
	return buffers, bytes
}
