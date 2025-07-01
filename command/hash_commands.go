package command

import (
	"github.com/liweiyuan/go-redis-server/resp"
	"github.com/liweiyuan/go-redis-server/storage"
)

func registerHashCommands(cr *CommandRegistry) {
	cr.register("HSET", NewHSetCommand)
	cr.register("HGET", NewHGetCommand)
	cr.register("HDEL", NewHDelCommand)
	cr.register("HEXISTS", NewHExistsCommand)
	cr.register("HLEN", NewHLenCommand)
	cr.register("HGETALL", NewHGetAllCommand)
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