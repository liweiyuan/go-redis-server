package command

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/liweiyuan/go-redis-server/resp"
	"github.com/liweiyuan/go-redis-server/storage"
)

// Command represents a Redis command.
type Command interface {
	Apply(s *storage.Storage) resp.RespValue
}

// CommandRegistry holds the mapping of command names to their implementations.
type CommandRegistry struct {
	commands map[string]func(args []resp.RespValue) (Command, error)
}

// NewCommandRegistry creates a new CommandRegistry.
func NewCommandRegistry() *CommandRegistry {
	cr := &CommandRegistry{
		commands: make(map[string]func(args []resp.RespValue) (Command, error)),
	}
	cr.register("PING", NewPingCommand)
	cr.register("SET", NewSetCommand)
	cr.register("GET", NewGetCommand)
	cr.register("DEL", NewDelCommand)
	cr.register("EXISTS", NewExistsCommand)
	cr.register("INCR", NewIncrCommand)
	cr.register("DECR", NewDecrCommand)
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
	cr.register("HSET", NewHSetCommand)
	cr.register("HGET", NewHGetCommand)
	cr.register("HDEL", NewHDelCommand)
	cr.register("HEXISTS", NewHExistsCommand)
	cr.register("HLEN", NewHLenCommand)
	cr.register("HGETALL", NewHGetAllCommand)
	cr.register("SADD", NewSAddCommand)
	cr.register("SREM", NewSRemCommand)
	cr.register("SISMEMBER", NewSIsMemberCommand)
	cr.register("SCARD", NewSCardCommand)
	cr.register("SMEMBERS", NewSMembersCommand)
	cr.register("SPOP", NewSPopCommand)
	cr.register("SRANDMEMBER", NewSRandMemberCommand)
	cr.register("SINTER", NewSInterCommand)
	cr.register("SUNION", NewSUnionCommand)
	cr.register("SDIFF", NewSDiffCommand)
	cr.register("ZADD", NewZAddCommand)
	cr.register("ZSCORE", NewZScoreCommand)
	cr.register("ZREM", NewZRemCommand)
	cr.register("ZCARD", NewZCardCommand)
	cr.register("ZRANGE", NewZRangeCommand)
	cr.register("ZRANGEBYSCORE", NewZRangeByScoreCommand)
	cr.register("ZCOUNT", NewZCountCommand)
	cr.register("ZINCRBY", NewZIncrByCommand)
	cr.register("ZRANK", NewZRankCommand)
	cr.register("ZREVRANK", NewZRevRankCommand)
	cr.register("ZREVRANGEBYSCORE", NewZRevRangeByScoreCommand)
	cr.register("ZREVRANGE", NewZRevRangeCommand)
	return cr
}

// register registers a new command.
func (cr *CommandRegistry) register(name string, constructor func(args []resp.RespValue) (Command, error)) {
	cr.commands[strings.ToUpper(name)] = constructor
}

// ParseCommand parses a RESP array into a Command.
func (cr *CommandRegistry) ParseCommand(respValue resp.RespValue) (Command, error) {
	if respValue.Type != resp.Array || len(respValue.Array) == 0 {
		return nil, resp.NewError("ERR invalid command format")
	}

	cmdName := strings.ToUpper(respValue.Array[0].Str)
	constructor, ok := cr.commands[cmdName]
	if !ok {
		return nil, resp.NewError(fmt.Sprintf("ERR unknown command '%s'", cmdName))
	}

	return constructor(respValue.Array[1:])
}

// PingCommand implements the PING command.
type PingCommand struct {
	message string
}

// NewPingCommand creates a new PingCommand.
func NewPingCommand(args []resp.RespValue) (Command, error) {
	if len(args) > 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'ping' command")
	}

	msg := "PONG"
	if len(args) == 1 {
		if args[0].Type != resp.Bulk {
			return nil, resp.NewError("ERR PING argument must be a bulk string")
		}
		msg = args[0].Str
	}
	return &PingCommand{message: msg}, nil
}

// Apply executes the PING command.
func (c *PingCommand) Apply(s *storage.Storage) resp.RespValue {
	return resp.NewString(c.message)
}

// SetCommand implements the SET command.
type SetCommand struct {
	key   string
	value string
}

// NewSetCommand creates a new SetCommand.
func NewSetCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'set' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk {
		return nil, resp.NewError("ERR SET arguments must be bulk strings")
	}

	return &SetCommand{key: args[0].Str, value: args[1].Str}, nil
}

// Apply executes the SET command.
func (c *SetCommand) Apply(s *storage.Storage) resp.RespValue {
	s.Set(c.key, c.value)
	return resp.NewString("OK")
}

// GetCommand implements the GET command.
type GetCommand struct {
	key string
}

// NewGetCommand creates a new GetCommand.
func NewGetCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'get' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR GET argument must be a bulk string")
	}

	return &GetCommand{key: args[0].Str}, nil
}

// Apply executes the GET command.
func (c *GetCommand) Apply(s *storage.Storage) resp.RespValue {
	val, ok := s.Get(c.key)
	if !ok {
		return resp.NewBulk("") // Return null bulk string if key not found
	}
	return resp.NewBulk(val)
}

// DelCommand implements the DEL command.
type DelCommand struct {
	keys []string
}

// NewDelCommand creates a new DelCommand.
func NewDelCommand(args []resp.RespValue) (Command, error) {
	if len(args) == 0 {
		return nil, resp.NewError("ERR wrong number of arguments for 'del' command")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR DEL arguments must be bulk strings")
		}
		keys[i] = arg.Str
	}
	return &DelCommand{keys: keys}, nil
}

// Apply executes the DEL command.
func (c *DelCommand) Apply(s *storage.Storage) resp.RespValue {
	count := s.Del(c.keys...)
	return resp.NewInteger(int64(count))
}

// ExistsCommand implements the EXISTS command.
type ExistsCommand struct {
	keys []string
}

// NewExistsCommand creates a new ExistsCommand.
func NewExistsCommand(args []resp.RespValue) (Command, error) {
	if len(args) == 0 {
		return nil, resp.NewError("ERR wrong number of arguments for 'exists' command")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR EXISTS arguments must be bulk strings")
		}
		keys[i] = arg.Str
	}
	return &ExistsCommand{keys: keys}, nil
}

// Apply executes the EXISTS command.
func (c *ExistsCommand) Apply(s *storage.Storage) resp.RespValue {
	count := s.Exists(c.keys...)
	return resp.NewInteger(int64(count))
}

// IncrCommand implements the INCR command.
type IncrCommand struct {
	key string
}

// NewIncrCommand creates a new IncrCommand.
func NewIncrCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'incr' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR INCR argument must be a bulk string")
	}

	return &IncrCommand{key: args[0].Str}, nil
}

// Apply executes the INCR command.
func (c *IncrCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.Incr(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(val)
}

// DecrCommand implements the DECR command.
type DecrCommand struct {
	key string
}

// NewDecrCommand creates a new DecrCommand.
func NewDecrCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'decr' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR DECR argument must be a bulk string")
	}

	return &DecrCommand{key: args[0].Str}, nil
}

// Apply executes the DECR command.
func (c *DecrCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.Decr(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(val)
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

// HSetCommand implements the HSET command.
type HSetCommand struct {
	key   string
	field string
	value string
}

// NewHSetCommand creates a new HSetCommand.
func NewHSetCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'hset' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR HSET arguments must be bulk strings")
	}

	return &HSetCommand{key: args[0].Str, field: args[1].Str, value: args[2].Str}, nil
}

// Apply executes the HSET command.
func (c *HSetCommand) Apply(s *storage.Storage) resp.RespValue {
	count, err := s.HSet(c.key, c.field, c.value)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(count)
}

// HGetCommand implements the HGET command.
type HGetCommand struct {
	key   string
	field string
}

// NewHGetCommand creates a new HGetCommand.
func NewHGetCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'hget' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk {
		return nil, resp.NewError("ERR HGET arguments must be bulk strings")
	}

	return &HGetCommand{key: args[0].Str, field: args[1].Str}, nil
}

// Apply executes the HGET command.
func (c *HGetCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.HGet(c.key, c.field)
	if err != nil {
		return resp.NewError(err.Error())
	}
	if val == "" {
		return resp.NewBulk("") // Return null bulk string if field not found
	}
	return resp.NewBulk(val)
}

// HDelCommand implements the HDEL command.
type HDelCommand struct {
	key    string
	fields []string
}

// NewHDelCommand creates a new HDelCommand.
func NewHDelCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'hdel' command")
	}

	key := args[0].Str
	fields := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR HDEL arguments must be bulk strings")
		}
		fields[i] = arg.Str
	}
	return &HDelCommand{key: key, fields: fields}, nil
}

// Apply executes the HDEL command.
func (c *HDelCommand) Apply(s *storage.Storage) resp.RespValue {
	count, err := s.HDel(c.key, c.fields...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(count)
}

// HExistsCommand implements the HEXISTS command.
type HExistsCommand struct {
	key   string
	field string
}

// NewHExistsCommand creates a new HExistsCommand.
func NewHExistsCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'hexists' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk {
		return nil, resp.NewError("ERR HEXISTS arguments must be bulk strings")
	}

	return &HExistsCommand{key: args[0].Str, field: args[1].Str}, nil
}

// Apply executes the HEXISTS command.
func (c *HExistsCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.HExists(c.key, c.field)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(val)
}

// HLenCommand implements the HLEN command.
type HLenCommand struct {
	key string
}

// NewHLenCommand creates a new HLenCommand.
func NewHLenCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'hlen' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR HLEN argument must be a bulk string")
	}

	return &HLenCommand{key: args[0].Str}, nil
}

// Apply executes the HLEN command.
func (c *HLenCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.HLen(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(val)
}

// HGetAllCommand implements the HGETALL command.
type HGetAllCommand struct {
	key string
}

// NewHGetAllCommand creates a new HGetAllCommand.
func NewHGetAllCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'hgetall' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR HGETALL argument must be a bulk string")
	}

	return &HGetAllCommand{key: args[0].Str}, nil
}

// Apply executes the HGETALL command.
func (c *HGetAllCommand) Apply(s *storage.Storage) resp.RespValue {
	values, err := s.HGetAll(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(values))
	for i, val := range values {
		respValues[i] = resp.NewBulk(val)
	}
	return resp.NewArray(respValues)
}

// SAddCommand implements the SADD command.
type SAddCommand struct {
	key     string
	members []string
}

// NewSAddCommand creates a new SAddCommand.
func NewSAddCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'sadd' command")
	}

	key := args[0].Str
	members := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR SADD arguments must be bulk strings")
		}
		members[i] = arg.Str
	}
	return &SAddCommand{key: key, members: members}, nil
}

// Apply executes the SADD command.
func (c *SAddCommand) Apply(s *storage.Storage) resp.RespValue {
	count, err := s.SAdd(c.key, c.members...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(count)
}

// SRemCommand implements the SREM command.
type SRemCommand struct {
	key     string
	members []string
}

// NewSRemCommand creates a new SRemCommand.
func NewSRemCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'srem' command")
	}

	key := args[0].Str
	members := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR SREM arguments must be bulk strings")
		}
		members[i] = arg.Str
	}
	return &SRemCommand{key: key, members: members}, nil
}

// Apply executes the SREM command.
func (c *SRemCommand) Apply(s *storage.Storage) resp.RespValue {
	count, err := s.SRem(c.key, c.members...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(count)
}

// SIsMemberCommand implements the SISMEMBER command.
type SIsMemberCommand struct {
	key    string
	member string
}

// NewSIsMemberCommand creates a new SIsMemberCommand.
func NewSIsMemberCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'sismember' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk {
		return nil, resp.NewError("ERR SISMEMBER arguments must be bulk strings")
	}

	return &SIsMemberCommand{key: args[0].Str, member: args[1].Str}, nil
}

// Apply executes the SISMEMBER command.
func (c *SIsMemberCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.SIsMember(c.key, c.member)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(val)
}

// SCardCommand implements the SCARD command.
type SCardCommand struct {
	key string
}

// NewSCardCommand creates a new SCardCommand.
func NewSCardCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'scard' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR SCARD argument must be a bulk string")
	}

	return &SCardCommand{key: args[0].Str}, nil
}

// Apply executes the SCARD command.
func (c *SCardCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.SCard(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(val)
}

// SMembersCommand implements the SMEMBERS command.
type SMembersCommand struct {
	key string
}

// NewSMembersCommand creates a new SMembersCommand.
func NewSMembersCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'smembers' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR SMEMBERS argument must be a bulk string")
	}

	return &SMembersCommand{key: args[0].Str}, nil
}

// Apply executes the SMEMBERS command.
func (c *SMembersCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.SMembers(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// SPopCommand implements the SPOP command.
type SPopCommand struct {
	key   string
	count int64
}

// NewSPopCommand creates a new SPopCommand.
func NewSPopCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'spop' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR SPOP argument must be a bulk string")
	}

	count := int64(1) // Default count is 1
	if len(args) == 2 {
		if args[1].Type != resp.Bulk {
			return nil, resp.NewError("ERR SPOP count argument must be an integer")
		}
		parsedCount, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return nil, resp.NewError("ERR value is not an integer or out of range")
		}
		count = parsedCount
	}

	return &SPopCommand{key: args[0].Str, count: count}, nil
}

// Apply executes the SPOP command.
func (c *SPopCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.SPop(c.key, c.count)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// SRandMemberCommand implements the SRANDMEMBER command.
type SRandMemberCommand struct {
	key   string
	count int64
}

// NewSRandMemberCommand creates a new SRandMemberCommand.
func NewSRandMemberCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'srandmember' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR SRANDMEMBER argument must be a bulk string")
	}

	count := int64(1) // Default count is 1
	if len(args) == 2 {
		if args[1].Type != resp.Bulk {
			return nil, resp.NewError("ERR SRANDMEMBER count argument must be an integer")
		}
		parsedCount, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return nil, resp.NewError("ERR value is not an integer or out of range")
		}
		count = parsedCount
	}

	return &SRandMemberCommand{key: args[0].Str, count: count}, nil
}

// Apply executes the SRANDMEMBER command.
func (c *SRandMemberCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.SRandMember(c.key, c.count)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// SInterCommand implements the SINTER command.
type SInterCommand struct {
	keys []string
}

// NewSInterCommand creates a new SInterCommand.
func NewSInterCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'sinter' command")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR SINTER arguments must be bulk strings")
		}
		keys[i] = arg.Str
	}
	return &SInterCommand{keys: keys}, nil
}

// Apply executes the SINTER command.
func (c *SInterCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.SInter(c.keys...)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// SUnionCommand implements the SUNION command.
type SUnionCommand struct {
	keys []string
}

// NewSUnionCommand creates a new SUnionCommand.
func NewSUnionCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'sunion' command")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR SUNION arguments must be bulk strings")
		}
		keys[i] = arg.Str
	}
	return &SUnionCommand{keys: keys}, nil
}

// Apply executes the SUNION command.
func (c *SUnionCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.SUnion(c.keys...)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// SDiffCommand implements the SDIFF command.
type SDiffCommand struct {
	keys []string
}

// NewSDiffCommand creates a new SDiffCommand.
func NewSDiffCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'sdiff' command")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR SDIFF arguments must be bulk strings")
		}
		keys[i] = arg.Str
	}
	return &SDiffCommand{keys: keys}, nil
}

// Apply executes the SDIFF command.
func (c *SDiffCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.SDiff(c.keys...)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// ZAddCommand implements the ZADD command.
type ZAddCommand struct {
	key     string
	members []storage.ZSetMember
}

// NewZAddCommand creates a new ZAddCommand.
func NewZAddCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zadd' command")
	}

	key := args[0].Str
	members := make([]storage.ZSetMember, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		if args[i].Type != resp.Bulk || args[i+1].Type != resp.Bulk {
			return nil, resp.NewError("ERR ZADD arguments must be bulk strings")
		}
		score, err := strconv.ParseFloat(args[i].Str, 64)
		if err != nil {
			return nil, resp.NewError("ERR value is not a valid float")
		}
		members[(i-1)/2] = storage.ZSetMember{Score: score, Member: args[i+1].Str}
	}
	return &ZAddCommand{key: key, members: members}, nil
}

// Apply executes the ZADD command.
func (c *ZAddCommand) Apply(s *storage.Storage) resp.RespValue {
	count, err := s.ZAdd(c.key, c.members...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(count)
}

// ZScoreCommand implements the ZSCORE command.
type ZScoreCommand struct {
	key    string
	member string
}

// NewZScoreCommand creates a new ZScoreCommand.
func NewZScoreCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zscore' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk {
		return nil, resp.NewError("ERR ZSCORE arguments must be bulk strings")
	}

	return &ZScoreCommand{key: args[0].Str, member: args[1].Str}, nil
}

// Apply executes the ZSCORE command.
func (c *ZScoreCommand) Apply(s *storage.Storage) resp.RespValue {
	score, found, err := s.ZScore(c.key, c.member)
	if err != nil {
		return resp.NewError(err.Error())
	}
	if !found {
		return resp.NewBulk("") // Return null bulk string if member not found
	}
	return resp.NewBulk(strconv.FormatFloat(score, 'f', -1, 64))
}

// ZRemCommand implements the ZREM command.
type ZRemCommand struct {
	key     string
	members []string
}

// NewZRemCommand creates a new ZRemCommand.
func NewZRemCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zrem' command")
	}

	key := args[0].Str
	members := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		if arg.Type != resp.Bulk {
			return nil, resp.NewError("ERR ZREM arguments must be bulk strings")
		}
		members[i] = arg.Str
	}
	return &ZRemCommand{key: key, members: members}, nil
}

// Apply executes the ZREM command.
func (c *ZRemCommand) Apply(s *storage.Storage) resp.RespValue {
	count, err := s.ZRem(c.key, c.members...)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(count)
}

// ZCardCommand implements the ZCARD command.
type ZCardCommand struct {
	key string
}

// NewZCardCommand creates a new ZCardCommand.
func NewZCardCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 1 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zcard' command")
	}

	if args[0].Type != resp.Bulk {
		return nil, resp.NewError("ERR ZCARD argument must be a bulk string")
	}

	return &ZCardCommand{key: args[0].Str}, nil
}

// Apply executes the ZCARD command.
func (c *ZCardCommand) Apply(s *storage.Storage) resp.RespValue {
	val, err := s.ZCard(c.key)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(val)
}

// ZRangeCommand implements the ZRANGE command.
type ZRangeCommand struct {
	key        string
	start      int64
	stop       int64
	withScores bool
}

// NewZRangeCommand creates a new ZRangeCommand.
func NewZRangeCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 3 || len(args) > 4 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zrange' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR ZRANGE arguments must be bulk strings")
	}

	start, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}

	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(args[3].Str) == "WITHSCORES" {
			withScores = true
		} else {
			return nil, resp.NewError("ERR syntax error")
		}
	}

	return &ZRangeCommand{key: args[0].Str, start: start, stop: stop, withScores: withScores}, nil
}

// Apply executes the ZRANGE command.
func (c *ZRangeCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.ZRange(c.key, c.start, c.stop, c.withScores)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// ZRangeByScoreCommand implements the ZRANGEBYSCORE command.
type ZRangeByScoreCommand struct {
	key        string
	min        float64
	max        float64
	offset     int64
	count      int64
	withScores bool
}

// NewZRangeByScoreCommand creates a new ZRangeByScoreCommand.
func NewZRangeByScoreCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zrangebyscore' command")
	}

	key := args[0].Str
	min, err := strconv.ParseFloat(args[1].Str, 64)
	if err != nil {
		return nil, resp.NewError("ERR min is not a valid float")
	}
	max, err := strconv.ParseFloat(args[2].Str, 64)
	if err != nil {
		return nil, resp.NewError("ERR max is not a valid float")
	}

	offset := int64(0)
	count := int64(-1) // -1 means no limit
	withScores := false

	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return nil, resp.NewError("ERR syntax error")
			}
			offset, err = strconv.ParseInt(args[i+1].Str, 10, 64)
			if err != nil {
				return nil, resp.NewError("ERR offset is not an integer or out of range")
			}
			count, err = strconv.ParseInt(args[i+2].Str, 10, 64)
			if err != nil {
				return nil, resp.NewError("ERR count is not an integer or out of range")
			}
			i += 2
		default:
			return nil, resp.NewError("ERR syntax error")
		}
	}

	return &ZRangeByScoreCommand{key: key, min: min, max: max, offset: offset, count: count, withScores: withScores}, nil
}

// Apply executes the ZRANGEBYSCORE command.
func (c *ZRangeByScoreCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.ZRangeByScore(c.key, c.min, c.max, c.offset, c.count, c.withScores)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// ZCountCommand implements the ZCOUNT command.
type ZCountCommand struct {
	key string
	min float64
	max float64
}

// NewZCountCommand creates a new ZCountCommand.
func NewZCountCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zcount' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR ZCOUNT arguments must be bulk strings")
	}

	min, err := strconv.ParseFloat(args[1].Str, 64)
	if err != nil {
		return nil, resp.NewError("ERR min is not a valid float")
	}
	max, err := strconv.ParseFloat(args[2].Str, 64)
	if err != nil {
		return nil, resp.NewError("ERR max is not a valid float")
	}

	return &ZCountCommand{key: args[0].Str, min: min, max: max}, nil
}

// Apply executes the ZCOUNT command.
func (c *ZCountCommand) Apply(s *storage.Storage) resp.RespValue {
	count, err := s.ZCount(c.key, c.min, c.max)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewInteger(count)
}

// ZIncrByCommand implements the ZINCRBY command.
type ZIncrByCommand struct {
	key       string
	increment float64
	member    string
}

// NewZIncrByCommand creates a new ZIncrByCommand.
func NewZIncrByCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zincrby' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR ZINCRBY arguments must be bulk strings")
	}

	increment, err := strconv.ParseFloat(args[1].Str, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not a valid float")
	}

	return &ZIncrByCommand{key: args[0].Str, increment: increment, member: args[2].Str}, nil
}

// Apply executes the ZINCRBY command.
func (c *ZIncrByCommand) Apply(s *storage.Storage) resp.RespValue {
	newScore, err := s.ZIncrBy(c.key, c.increment, c.member)
	if err != nil {
		return resp.NewError(err.Error())
	}
	return resp.NewBulk(strconv.FormatFloat(newScore, 'f', -1, 64))
}

// ZRankCommand implements the ZRANK command.
type ZRankCommand struct {
	key    string
	member string
}

// NewZRankCommand creates a new ZRankCommand.
func NewZRankCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zrank' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk {
		return nil, resp.NewError("ERR ZRANK arguments must be bulk strings")
	}

	return &ZRankCommand{key: args[0].Str, member: args[1].Str}, nil
}

// Apply executes the ZRANK command.
func (c *ZRankCommand) Apply(s *storage.Storage) resp.RespValue {
	rank, found, err := s.ZRank(c.key, c.member)
	if err != nil {
		return resp.NewError(err.Error())
	}
	if !found {
		return resp.NewBulk("") // Return null bulk string if member not found
	}
	return resp.NewInteger(rank)
}

// ZRevRankCommand implements the ZREVRANK command.
type ZRevRankCommand struct {
	key    string
	member string
}

// NewZRevRankCommand creates a new ZRevRankCommand.
func NewZRevRankCommand(args []resp.RespValue) (Command, error) {
	if len(args) != 2 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zrevrank' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk {
		return nil, resp.NewError("ERR ZREVRANK arguments must be bulk strings")
	}

	return &ZRevRankCommand{key: args[0].Str, member: args[1].Str}, nil
}

// Apply executes the ZREVRANK command.
func (c *ZRevRankCommand) Apply(s *storage.Storage) resp.RespValue {
	rank, found, err := s.ZRevRank(c.key, c.member)
	if err != nil {
		return resp.NewError(err.Error())
	}
	if !found {
		return resp.NewBulk("") // Return null bulk string if member not found
	}
	return resp.NewInteger(rank)
}

// ZRevRangeByScoreCommand implements the ZREVRANGEBYSCORE command.
type ZRevRangeByScoreCommand struct {
	key        string
	max        float64
	min        float64
	offset     int64
	count      int64
	withScores bool
}

// NewZRevRangeByScoreCommand creates a new ZRevRangeByScoreCommand.
func NewZRevRangeByScoreCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 3 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zrevrangebyscore' command")
	}

	key := args[0].Str
	max, err := strconv.ParseFloat(args[1].Str, 64)
	if err != nil {
		return nil, resp.NewError("ERR max is not a valid float")
	}
	min, err := strconv.ParseFloat(args[2].Str, 64)
	if err != nil {
		return nil, resp.NewError("ERR min is not a valid float")
	}

	offset := int64(0)
	count := int64(-1) // -1 means no limit
	withScores := false

	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return nil, resp.NewError("ERR syntax error")
			}
			offset, err = strconv.ParseInt(args[i+1].Str, 10, 64)
			if err != nil {
				return nil, resp.NewError("ERR offset is not an integer or out of range")
			}
			count, err = strconv.ParseInt(args[i+2].Str, 10, 64)
			if err != nil {
				return nil, resp.NewError("ERR count is not an integer or out of range")
			}
			i += 2
		default:
			return nil, resp.NewError("ERR syntax error")
		}
	}

	return &ZRevRangeByScoreCommand{key: key, max: max, min: min, offset: offset, count: count, withScores: withScores}, nil
}

// Apply executes the ZREVRANGEBYSCORE command.
func (c *ZRevRangeByScoreCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.ZRevRangeByScore(c.key, c.max, c.min, c.offset, c.count, c.withScores)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}

// ZRevRangeCommand implements the ZREVRANGE command.
type ZRevRangeCommand struct {
	key        string
	start      int64
	stop       int64
	withScores bool
}

// NewZRevRangeCommand creates a new ZRevRangeCommand.
func NewZRevRangeCommand(args []resp.RespValue) (Command, error) {
	if len(args) < 3 || len(args) > 4 {
		return nil, resp.NewError("ERR wrong number of arguments for 'zrevrange' command")
	}

	if args[0].Type != resp.Bulk || args[1].Type != resp.Bulk || args[2].Type != resp.Bulk {
		return nil, resp.NewError("ERR ZREVRANGE arguments must be bulk strings")
	}

	start, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2].Str, 10, 64)
	if err != nil {
		return nil, resp.NewError("ERR value is not an integer or out of range")
	}

	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(args[3].Str) == "WITHSCORES" {
			withScores = true
		} else {
			return nil, resp.NewError("ERR syntax error")
		}
	}

	return &ZRevRangeCommand{key: args[0].Str, start: start, stop: stop, withScores: withScores}, nil
}

// Apply executes the ZREVRANGE command.
func (c *ZRevRangeCommand) Apply(s *storage.Storage) resp.RespValue {
	members, err := s.ZRevRange(c.key, c.start, c.stop, c.withScores)
	if err != nil {
		return resp.NewError(err.Error())
	}

	respValues := make([]resp.RespValue, len(members))
	for i, member := range members {
		respValues[i] = resp.NewBulk(member)
	}
	return resp.NewArray(respValues)
}
