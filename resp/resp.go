package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP types
const (
	String  = '+'
	Error   = '-'
	Integer = ':'
	Bulk    = '$'
	Array   = '*'
)

// RespValue represents a parsed RESP value
type RespValue struct {
	Type  byte
	Str   string
	Num   int64
	Array []RespValue
}

func (e RespValue) Error() string {
	return e.Str
}

// NewString creates a new RESP string value
func NewString(s string) RespValue {
	return RespValue{Type: String, Str: s}
}

// NewError creates a new RESP error value
func NewError(s string) RespValue {
	return RespValue{Type: Error, Str: s}
}

// NewInteger creates a new RESP integer value
func NewInteger(i int64) RespValue {
	return RespValue{Type: Integer, Num: i}
}

// NewBulk creates a new RESP bulk string value
func NewBulk(s string) RespValue {
	return RespValue{Type: Bulk, Str: s}
}

// NewArray creates a new RESP array value
func NewArray(arr []RespValue) RespValue {
	return RespValue{Type: Array, Array: arr}
}

// ReadResp reads a RESP value from the given reader
func ReadResp(reader *bufio.Reader) (RespValue, error) {
	typeByte, err := reader.ReadByte()
	if err != nil {
		return RespValue{}, err
	}

	switch typeByte {
	case String:
		return readSimpleString(reader)
	case Error:
		return readError(reader)
	case Integer:
		return readInteger(reader)
	case Bulk:
		return readBulkString(reader)
	case Array:
		return readArray(reader)
	default:
		return RespValue{}, fmt.Errorf("unknown RESP type: %c", typeByte)
	}
}

func readSimpleString(reader *bufio.Reader) (RespValue, error) {
	s, err := readLine(reader)
	if err != nil {
		return RespValue{}, err
	}
	return NewString(s), nil
}

func readError(reader *bufio.Reader) (RespValue, error) {
	s, err := readLine(reader)
	if err != nil {
		return RespValue{}, err
	}
	return NewError(s), nil
}

func readInteger(reader *bufio.Reader) (RespValue, error) {
	s, err := readLine(reader)
	if err != nil {
		return RespValue{}, err
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return RespValue{}, err
	}
	return NewInteger(i), nil
}

func readBulkString(reader *bufio.Reader) (RespValue, error) {
	lenStr, err := readLine(reader)
	if err != nil {
		return RespValue{}, err
	}
	length, err := strconv.Atoi(lenStr)
	if err != nil {
		return RespValue{}, err
	}

	if length == -1 {
		return NewBulk(""), nil // Null bulk string
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return RespValue{}, err
	}

	// Read the trailing CRLF
	_, err = reader.ReadByte() // \r
	if err != nil {
		return RespValue{}, err
	}
	_, err = reader.ReadByte() // \n
	if err != nil {
		return RespValue{}, err
	}

	return NewBulk(string(buf)), nil
}

func readArray(reader *bufio.Reader) (RespValue, error) {
	lenStr, err := readLine(reader)
	if err != nil {
		return RespValue{}, err
	}
	length, err := strconv.Atoi(lenStr)
	if err != nil {
		return RespValue{}, err
	}

	if length == -1 {
		return NewArray(nil), nil // Null array
	}

	arr := make([]RespValue, length)
	for i := 0; i < length; i++ {
		val, err := ReadResp(reader)
		if err != nil {
			return RespValue{}, err
		}
		arr[i] = val
	}
	return NewArray(arr), nil
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line[:len(line)-2], nil // Remove CRLF
}

// WriteResp writes a RESP value to the given writer
func WriteResp(writer io.Writer, val RespValue) error {
	switch val.Type {
	case String:
		_, err := fmt.Fprintf(writer, "+%s\r\n", val.Str)
		return err
	case Error:
		_, err := fmt.Fprintf(writer, "-%s\r\n", val.Str)
		return err
	case Integer:
		_, err := fmt.Fprintf(writer, ":%d\r\n", val.Num)
		return err
	case Bulk:
		_, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(val.Str), val.Str)
		return err
	case Array:
		_, err := fmt.Fprintf(writer, "*%d\r\n", len(val.Array))
		if err != nil {
			return err
		}
		for _, item := range val.Array {
			err := WriteResp(writer, item)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown RESP type to write: %c", val.Type)
	}
}
