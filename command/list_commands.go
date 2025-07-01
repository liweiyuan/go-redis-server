package command

import (
	"strconv"
	"strings"

	"github.com/liweiyuan/go-redis-server/resp"
	"github.com/liweiyuan/go-redis-server/storage"
)

func registerListCommands(cr *CommandRegistry) {
	cr.register("LPUSH", NewLPushCommand)
	cr.register("RPUSH", NewRPushCommand)
	cr.register("LPOP", NewLPopCommand)
	cr.register("RPOP", NewRPopCommand)
	cr.register("LLEN", NewLLenCommand)
	cr.register("LINDEX", NewLIndexCommand)
	cr.register("LSET", NewLSetCommand)
	cr.register("LREM", NewLRemCommand)
	cr.register("LPUSHX", NewLPushXCommand)
	cr.register("RPUSHX", NewRPushXCommand)
	cr.register("LINSERT", NewLInsertCommand)
	cr.register("LRANGE", NewLRangeCommand)
	cr.register("LTRIM", NewLTrimCommand)
}

// LPushCommand implements the LPUSH command.
type LPushCommand struct {
	key    string
	values []string
}

// NewLPushCommand creates a new LPushCommand.
func NewLPushCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'lpush' command")
	}

	key := args[0].Str
	values := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR LPUSH arguments must be bulk strings")
		}
		values[i] = arg.Str
	}
	return &LPushCommand{key: key, values: values}, nil
}

// Apply executes the LPUSH command.
func (c *LPushCommand) Apply(s *storage.Storage) resp.RespValue {
	length, err := s.LPush(c.key, c.values...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(length)
}

// RPushCommand implements the RPUSH command.
type RPushCommand struct {
	key    string
	values []string
}

// NewRPushCommand creates a new RPushCommand.
func NewRPushCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'rpush' command")
	}

	key := args[0].Str
	values := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR RPUSH arguments must be bulk strings")
		}
		values[i] = arg.Str
	}
	return &RPushCommand{key: key, values: values}, nil
}

// Apply executes the RPUSH command.
func (c *RPushCommand) Apply(s *storage.Storage) resp.RespValue {
	length, err := s.RPush(c.key, c.values...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(length)
}

// LPopCommand implements the LPOP command.
type LPopCommand struct {
	key string
}

// NewLPopCommand creates a new LPopCommand.
func NewLPopCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'lpop' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR LPOP argument must be a bulk string")
	}

	return &LPopCommand{key: args[0].Str}, nil
}

// Apply executes the LPOP command.
func (c *LPopCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.LPop(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}
	if val == "" {
		return resp.NewBulk("") // Return null bulk string if no element
	}
	return resp.NewBulk(val)
}

// RPopCommand implements the RPOP command.
type RPopCommand struct {
	key string
}

// NewRPopCommand creates a new RPopCommand.
func NewRPopCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'rpop' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR RPOP argument must be a bulk string")
	}

	return &RPopCommand{key: args[0].Str}, nil
}

// Apply executes the RPOP command.
func (c *RPopCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.RPop(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}
	if val == "" {
		return resp.NewBulk("") // Return null bulk string if no element
	}
	return resp.NewBulk(val)
}

// LLenCommand implements the LLEN command.
type LLenCommand struct {
	key string
}

// NewLLenCommand creates a new LLenCommand.
func NewLLenCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'llen' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR LLEN argument must be a bulk string")
	}

	return &LLenCommand{key: args[0].Str}, nil
}

// Apply executes the LLEN command.
func (c *LLenCommand) Apply(s *storage.Storage) resp.RespValue {
	length, err := s.LLen(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(length)
}

// LIndexCommand implements the LINDEX command.
type LIndexCommand struct {
	key   string
	index int64
}

// NewLIndexCommand creates a new LIndexCommand.
func NewLIndexCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'lindex' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk {
		return nil, resp.NewError("ERR LINDEX arguments must be bulk strings")
	}

	index, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}

	return &LIndexCommand{key: args[0].Str, index: index}, nil
}

// Apply executes the LINDEX command.
func (c *LIndexCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.LIndex(c.key, c.index)
	if err != nil {
		return resp.NewError(err.Error())
	}
	if val == "" {
		return resp.NewBulk("") // Return null bulk string if no element or out of range
	}
	return resp.NewBulk(val)
}

// LSetCommand implements the LSET command.
type LSetCommand struct {
	key   string
	index int64
	value string
}

// NewLSetCommand creates a new LSetCommand.
func NewLSetCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'lset' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR LSET arguments must be bulk strings")
	}

	index, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}

	return &LSetCommand{key: args[0].Str, index: index, value: args[2].Str}, nil
}

// Apply executes the LSET command.
func (c *LSetCommand) Apply(s *storage.Storage) resp.RespValue {
	err := s.LSet(c.key, c.index, c.value)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewString("OK")
}

// LRemCommand implements the LREM command.
type LRemCommand struct {
	key   string
	count int64
	value string
}

// NewLRemCommand creates a new LRemCommand.
func NewLRemCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'lrem' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR LREM arguments must be bulk strings")
	}

	count, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}

	return &LRemCommand{key: args[0].Str, count: count, value: args[2].Str}, nil
}

// Apply executes the LREM command.
func (c *LRemCommand) Apply(s *storage.Storage) resp.RespValue {
	removed, err := s.LRem(c.key, c.count, c.value)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(removed)
}

// LPushXCommand implements the LPUSHX command.
type LPushXCommand struct {
	key    string
	values []string
}

// NewLPushXCommand creates a new LPushXCommand.
func NewLPushXCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'lpushx' command")
	}

	key := args[0].Str
	values := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR LPUSHX arguments must be bulk strings")
		}
		values[i] = arg.Str
	}
	return &LPushXCommand{key: key, values: values}, nil
}

// Apply executes the LPUSHX command.
func (c *LPushXCommand) Apply(s *storage.Storage) resp.RespValue {
	length, err := s.LPushX(c.key, c.values...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(length)
}

// RPushXCommand implements the RPUSHX command.
type RPushXCommand struct {
	key    string
	values []string
}

// NewRPushXCommand creates a new RPushXCommand.
func NewRPushXCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'rpushx' command")
	}

	key := args[0].Str
	values := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR RPUSHX arguments must be bulk strings")
		}
		values[i] = arg.Str
	}
	return &RPushXCommand{key: key, values: values}, nil
}

// Apply executes the RPUSHX command.
func (c *RPushXCommand) Apply(s *storage.Storage) resp.RespValue {
	length, err := s.RPushX(c.key, c.values...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(length)
}

// LInsertCommand implements the LINSERT command.
type LInsertCommand struct {
	key      string
	position string
	pivot    string
	value    string
}

// NewLInsertCommand creates a new LInsertCommand.
func NewLInsertCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 4 {
		return nil, resp.NewError("ERR wrong number of arguments for 'linsert' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk || args[3].Type != resp.Bulk {
		return nil, resp.NewError("ERR LINSERT arguments must be bulk strings")
	}

	position := strings.ToUpper(args[1].Str)
	if position != "BEFORE" && position != "AFTER" {
		return nil, resp.NewError("ERR syntax error")
	}

	return &LInsertCommand{key: args[0].Str, position: position, pivot: args[2].Str, value: args[3].Str}, nil
}

// Apply executes the LINSERT command.
func (c *LInsertCommand) Apply(s *storage.Storage) resp.RespValue {
	length, err := s.LInsert(c.key, c.position, c.pivot, c.value)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(length)
}

// LRangeCommand implements the LRANGE command.
type LRangeCommand struct {
	key   string
	start int64
	stop  int64
}

// NewLRangeCommand creates a new LRangeCommand.
func NewLRangeCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'lrange' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR LRANGE arguments must be bulk strings")
	}

	start, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}

	return &LRangeCommand{key: args[0].Str, start: start, stop: stop}, nil
}

// Apply executes the LRANGE command.
func (c *LRangeCommand) Apply(s *storage.Storage) resp.RespValue {
	values, err := s.LRange(c.key, c.start, c.stop)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(values))
	for i, val := range values {
		respValues[i] = resp.NewBulk(val)
	}
	return resp.NewArray(respValues)
}

// LTrimCommand implements the LTRIM command.
type LTrimCommand struct {
	key   string
	start int64
	stop  int64
}

// NewLTrimCommand creates a new LTrimCommand.
func NewLTrimCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'ltrim' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR LTRIM arguments must be bulk strings")
	}

	start, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}

	return &LTrimCommand{key: args[0].Str, start: start, stop: stop}, nil
}

// Apply executes the LTRIM command.
func (c *LTrimCommand) Apply(s *storage.Storage) resp.RespValue {
	err := s.LTrim(c.key, c.start, c.stop)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewString("OK")
}