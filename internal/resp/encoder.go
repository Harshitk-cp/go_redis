package resp

import (
	"fmt"
	"io"
)

func EncodeSimpleString(w io.Writer, message string) error {
	_, err := w.Write([]byte("+" + message + "\r\n"))
	return err
}

func EncodeError(w io.Writer, message string) error {
	_, err := w.Write([]byte("-" + message + "\r\n"))
	return err
}

func EncodeInteger(w io.Writer, value int) error {
	_, err := w.Write([]byte(fmt.Sprintf(":%d\r\n", value)))
	return err
}

func EncodeBulkString(w io.Writer, message string) error {
	_, err := w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)))
	return err
}

func EncodeArray(w io.Writer, elements []string) error {
	if _, err := w.Write([]byte(fmt.Sprintf("*%d\r\n", len(elements)))); err != nil {
		return err
	}
	for _, el := range elements {
		if err := EncodeBulkString(w, el); err != nil {
			return err
		}
	}
	return nil
}
