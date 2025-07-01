package command

import (
	"fmt"
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
	registerStringCommands(cr)
	registerListCommands(cr)
	registerHashCommands(cr)
	registerSetCommands(cr)
	registerSortedSetCommands(cr)
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