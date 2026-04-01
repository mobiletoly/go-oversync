package oversync

import "fmt"

const (
	opCodeInsert int16 = 1
	opCodeUpdate int16 = 2
	opCodeDelete int16 = 3
)

func opCodeFromString(op string) (int16, error) {
	switch op {
	case OpInsert:
		return opCodeInsert, nil
	case OpUpdate:
		return opCodeUpdate, nil
	case OpDelete:
		return opCodeDelete, nil
	default:
		return 0, fmt.Errorf("unsupported op %q", op)
	}
}

func opStringFromCode(code int16) (string, error) {
	switch code {
	case opCodeInsert:
		return OpInsert, nil
	case opCodeUpdate:
		return OpUpdate, nil
	case opCodeDelete:
		return OpDelete, nil
	default:
		return "", fmt.Errorf("unsupported op_code %d", code)
	}
}
