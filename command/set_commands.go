package command

import (
	"strconv"

	"github.com/liweiyuan/go-redis-server/resp"
	"github.com/liweiyuan/go-redis-server/storage"
)

func registerSetCommands(cr *CommandRegistry) {
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