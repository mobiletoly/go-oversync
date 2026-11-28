// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

const (
	// pullPageTargetDecodedBytes is the decoded size a pull page of more than
	// one bundle may have. A pull response is held in memory as a whole while
	// its bundles are applied, at several times its decoded size, so a page is
	// sized by bytes and not only by a bundle count.
	pullPageTargetDecodedBytes int64 = 4 << 20
	// pullPageInitialBundles is asked for before the size of this account's
	// bundles is known.
	pullPageInitialBundles = 16
	// pullPageGrowthFactor is the largest factor by which one request may
	// exceed the previous one, and the factor by which a refused request
	// shrinks.
	pullPageGrowthFactor = 4
)

// pullPageSizing chooses how many bundles each request of one incremental pull
// asks for.
//
// The server pages by bundle count and by its own response limit, and a bundle
// can hold anything from one row to the bundle byte limit. The first request
// is therefore small; each decoded page then tells how many bundles of that
// size fill half the byte target, and the next request asks for that many,
// growing by at most pullPageGrowthFactor per page. A page of several bundles
// above the target is refused before its JSON is parsed and asked for again
// with fewer bundles.
//
// One bundle is indivisible. A request for a single bundle is limited only by
// what the server advertised, so a bundle larger than the target is still
// pulled.
type pullPageSizing struct {
	maxBundles         int
	targetDecodedBytes int64
	requestBundles     int
}

// newPullPageSizing starts sizing for one pull. maxBundles and
// targetDecodedBytes must be positive.
func newPullPageSizing(maxBundles int, targetDecodedBytes int64) *pullPageSizing {
	return &pullPageSizing{
		maxBundles:         maxBundles,
		targetDecodedBytes: targetDecodedBytes,
		requestBundles:     min(maxBundles, pullPageInitialBundles),
	}
}

// responseDecodedByteLimit is the body limit for the next request, given the
// limit the server advertised.
func (p *pullPageSizing) responseDecodedByteLimit(admittedDecodedBytes int64) int64 {
	if p.requestBundles == 1 {
		return admittedDecodedBytes
	}
	return min(admittedDecodedBytes, p.targetDecodedBytes)
}

// pageDecoded records a decoded page and sizes the next request from it.
func (p *pullPageSizing) pageDecoded(bundles int, decodedBytes int64) {
	if bundles <= 0 || decodedBytes <= 0 {
		return
	}
	aimBytes := max(p.targetDecodedBytes/2, 1)
	bytesPerBundle := (decodedBytes-1)/int64(bundles) + 1 // rounded up, never zero
	fitting := aimBytes / bytesPerBundle
	ceiling := min(int64(p.maxBundles), int64(p.requestBundles)*pullPageGrowthFactor)
	p.requestBundles = int(min(max(fitting, 1), ceiling))
}

// shrinkAfterOversizedPage shrinks the request after a page was refused for
// exceeding the byte target. It reports false when the request is already a
// single bundle and cannot shrink.
func (p *pullPageSizing) shrinkAfterOversizedPage() bool {
	if p.requestBundles == 1 {
		return false
	}
	p.requestBundles = max(1, p.requestBundles/pullPageGrowthFactor)
	return true
}
