package server

import (
	"crypto/sha256"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
)

func retryableWriteOptionsForTest() oversync.RetryableWriteOptions {
	operationID := uuid.New()
	return oversync.RetryableWriteOptions{
		OperationID:         operationID,
		OperationHash:       sha256.Sum256(operationID[:]),
		OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(24 * time.Hour),
	}
}

func retryableBundleWriteOptionsForTest() oversync.RetryableBundleWriteOptions {
	seed := uuid.New()
	return oversync.RetryableBundleWriteOptions{
		OperationHash:       sha256.Sum256(seed[:]),
		OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(24 * time.Hour),
	}
}
