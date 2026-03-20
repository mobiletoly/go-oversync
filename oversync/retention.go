// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"errors"
	"fmt"
)

var errHistoryPruned = errors.New("requested checkpoint is older than retained history")

type HistoryPrunedError struct {
	UserID        string
	ProvidedSeq   int64
	RetainedFloor int64
}

func (e *HistoryPrunedError) Error() string {
	return fmt.Sprintf(
		"requested checkpoint %d is older than retained history floor %d for user %s",
		e.ProvidedSeq,
		e.RetainedFloor,
		e.UserID,
	)
}

func (e *HistoryPrunedError) Is(target error) bool {
	return target == errHistoryPruned
}
