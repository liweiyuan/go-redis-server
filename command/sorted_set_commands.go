package command

import (
	"strconv"
	"strings"

	"github.com/liweiyuan/go-redis-server/resp"
	"github.com/liweiyuan/go-redis-server/storage"
)

func registerSortedSetCommands(cr *CommandRegistry) {
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