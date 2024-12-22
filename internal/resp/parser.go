package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

func Parse(conn io.Reader) ([]string, error) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSuffix(line, "\r\n")

	if len(line) > 0 && line[0] == '*' {
		numArgs, err := strconv.Atoi(line[1:])
		if err != nil || numArgs <= 0 {
			return nil, errors.New("invalid RESP array format")
		}
		return parseArray(reader, numArgs)
	}
	return nil, errors.New("invalid RESP input")
}

func parseArray(reader *bufio.Reader, numArgs int) ([]string, error) {
	result := make([]string, 0, numArgs)

	for i := 0; i < numArgs; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSuffix(line, "\r\n")

		if len(line) > 0 && line[0] == '$' {
			argLength, err := strconv.Atoi(line[1:])
			if err != nil || argLength < 0 {
				return nil, errors.New("invalid bulk string format")
			}

			arg := make([]byte, argLength)
			if _, err := io.ReadFull(reader, arg); err != nil {
				return nil, err
			}

			if _, err := reader.Discard(2); err != nil {
				return nil, err
			}
			result = append(result, string(arg))
		} else {
			return nil, errors.New("expected bulk string in RESP array")
		}
	}
	return result, nil
}
