package command

import (
	"github.com/liweiyuan/go-redis-server/resp"
	"github.com/liweiyuan/go-redis-server/storage"
)

func registerStringCommands(cr *CommandRegistry) {
	cr.register("PING", NewPingCommand)
	cr.register("SET", NewSetCommand)
	cr.register("GET", NewGetCommand)
	cr.register("DEL", NewDelCommand)
	cr.register("EXISTS", NewExistsCommand)
	cr.register("INCR", NewIncrCommand)
	cr.register("DECR", NewDecrCommand)
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