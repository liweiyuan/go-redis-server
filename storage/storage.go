package storage

import (
	"container/list"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ZSetMember represents a member in a sorted set with its score.
type ZSetMember struct {
	Member string
	Score  float64
}

// Storage represents the in-memory key-value store.
type Storage struct {
	data sync.Map // Stores key-value pairs
}

// NewStorage creates a new Storage instance.
func NewStorage() *Storage {
	return &Storage{}
}

// Set sets a key-value pair in the storage.
func (s *Storage) Set(key, value string) {
	s.data.Store(key, value)
}

// Get retrieves the value associated with a key from the storage.
func (s *Storage) Get(key string) (string, bool) {
	if val, ok := s.data.Load(key); ok {
		// If it's a list, return an error as GET is for strings
		if _, isList := val.(*list.List); isList {
			return "", false // Or return an error type if we want to distinguish
		}
		return val.(string), true
	}
	return "", false
}

// Del deletes one or more keys from the storage.
func (s *Storage) Del(keys ...string) int {
	count := 0
	for _, key := range keys {
		if _, loaded := s.data.LoadAndDelete(key); loaded {
			count++
		}
	}
	return count
}

// Exists checks if one or more keys exist in the storage.
func (s *Storage) Exists(keys ...string) int {
	count := 0
	for _, key := range keys {
		if _, ok := s.data.Load(key); ok {
			count++
		}
	}
	return count
}

// Incr increments the integer value of a key by 1.
// If the key does not exist, it is set to 0 before performing the operation.
// If the key contains a value of the wrong type, an error is returned.
func (s *Storage) Incr(key string) (int64, error) {
	val, ok := s.Get(key)
	var num int64
	if !ok {
		num = 0
	} else {
		var err error
		num, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer or out of range")
		}
	}
	num++
	s.Set(key, strconv.FormatInt(num, 10))
	return num, nil
}

// Decr decrements the integer value of a key by 1.
// If the key does not exist, it is set to 0 before performing the operation.
// If the key contains a value of the wrong type, an error is returned.
func (s *Storage) Decr(key string) (int64, error) {
	val, ok := s.Get(key)
	var num int64
	if !ok {
		num = 0
	} else {
		var err error
		num, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer or out of range")
		}
	}
	num--
	s.Set(key, strconv.FormatInt(num, 10))
	return num, nil
}

// LPush prepends one or multiple values to a list.
func (s *Storage) LPush(key string, values ...string) (int64, error) {
	actual, _ := s.data.LoadOrStore(key, list.New())
	lst, ok := actual.(*list.List)
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	for _, val := range values {
		lst.PushFront(val)
	}
	return int64(lst.Len()), nil
}

// RPush appends one or multiple values to a list.
func (s *Storage) RPush(key string, values ...string) (int64, error) {
	actual, _ := s.data.LoadOrStore(key, list.New())
	lst, ok := actual.(*list.List)
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	for _, val := range values {
		lst.PushBack(val)
	}
	return int64(lst.Len()), nil
}

// LPop removes and returns the first element of the list stored at key.
func (s *Storage) LPop(key string) (string, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if lst.Len() == 0 {
			return "", nil // List is empty
		}
		elem := lst.Remove(lst.Front())
		return elem.(string), nil
	}
	return "", nil // Key not found
}

// RPop removes and returns the last element of the list stored at key.
func (s *Storage) RPop(key string) (string, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if lst.Len() == 0 {
			return "", nil // List is empty
		}
		elem := lst.Remove(lst.Back())
		return elem.(string), nil
	}
	return "", nil // Key not found
}

// LLen returns the length of the list stored at key.
func (s *Storage) LLen(key string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return int64(lst.Len()), nil
	}
	return 0, nil // Key not found, length is 0
}

// LIndex returns the element at index from the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list.
// Here, -1 means the last element, -2 means the penultimate and so on.
func (s *Storage) LIndex(key string, index int64) (string, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if lst.Len() == 0 {
			return "", nil // List is empty
		}

		// Adjust negative index
		if index < 0 {
			index = int64(lst.Len()) + index
		}

		if index < 0 || index >= int64(lst.Len()) {
			return "", nil // Index out of range
		}

		elem := lst.Front()
		for i := int64(0); i < index; i++ {
			elem = elem.Next()
		}
		return elem.Value.(string), nil
	}
	return "", nil // Key not found
}

// LSet sets the list element at index to value.
// An error is returned when the key is not a list or the index is out of range.
func (s *Storage) LSet(key string, index int64, value string) error {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		// Adjust negative index
		if index < 0 {
			index = int64(lst.Len()) + index
		}

		if index < 0 || index >= int64(lst.Len()) {
			return fmt.Errorf("ERR index out of range")
		}

		elem := lst.Front()
		for i := int64(0); i < index; i++ {
			elem = elem.Next()
		}
		elem.Value = value
		return nil
	}
	return fmt.Errorf("ERR no such key")
}

// LRem removes the first count occurrences of elements equal to value from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value.
func (s *Storage) LRem(key string, count int64, value string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		removed := int64(0)
		if count == 0 {
			// Remove all occurrences
			for e := lst.Front(); e != nil; {
				next := e.Next()
				if e.Value.(string) == value {
					lst.Remove(e)
					removed++
				}
				e = next
			}
		} else if count > 0 {
			// Remove from head to tail
			for e := lst.Front(); e != nil && removed < count; {
				next := e.Next()
				if e.Value.(string) == value {
					lst.Remove(e)
					removed++
				}
				e = next
			}
		} else { // count < 0
			// Remove from tail to head
			count = -count // Make count positive for iteration
			for e := lst.Back(); e != nil && removed < count; {
				prev := e.Prev()
				if e.Value.(string) == value {
					lst.Remove(e)
					removed++
				}
				e = prev
			}
		}
		return removed, nil
	}
	return 0, nil // Key not found
}

// LPushX prepends one or multiple values to a list only if the key already exists and holds a list.
func (s *Storage) LPushX(key string, values ...string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		for _, val := range values {
			lst.PushFront(val)
		}
		return int64(lst.Len()), nil
	}
	return 0, nil // Key not found, return 0 as per Redis behavior
}

// RPushX appends one or multiple values to a list only if the key already exists and holds a list.
func (s *Storage) RPushX(key string, values ...string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		for _, val := range values {
			lst.PushBack(val)
		}
		return int64(lst.Len()), nil
	}
	return 0, nil // Key not found, return 0 as per Redis behavior
}

// LInsert inserts an element before or after a pivot element in the list.
func (s *Storage) LInsert(key, position, pivot, value string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		found := false
		for e := lst.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == pivot {
				if position == "BEFORE" {
					lst.InsertBefore(value, e)
				} else if position == "AFTER" {
					lst.InsertAfter(value, e)
				}
				found = true
				break
			}
		}

		if !found {
			return -1, nil // Pivot not found
		}
		return int64(lst.Len()), nil
	}
	return 0, nil // Key not found
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes.
// Negative indices can be used to designate elements starting at the tail of the list.
func (s *Storage) LRange(key string, start, stop int64) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		length := int64(lst.Len())

		// Adjust negative indices
		if start < 0 {
			start = length + start
		}
		if stop < 0 {
			stop = length + stop
		}

		// Handle out of bounds indices
		if start < 0 {
			start = 0
		}
		if stop >= length {
			stop = length - 1
		}

		if start > stop || length == 0 {
			return []string{}, nil // Empty list or invalid range
		}

		var result []string
		elem := lst.Front()
		for i := int64(0); i < start; i++ {
			elem = elem.Next()
		}

		for i := start; i <= stop && elem != nil; i++ {
			result = append(result, elem.Value.(string))
			elem = elem.Next()
		}
		return result, nil
	}
	return []string{}, nil // Key not found, return empty list
}

// LTrim trims a list to the specified range of elements.
// The offsets start and stop are zero-based indexes.
// Negative indices can be used to designate elements starting at the tail of the list.
func (s *Storage) LTrim(key string, start, stop int64) error {
	if actual, ok := s.data.Load(key); ok {
		lst, ok := actual.(*list.List)
		if !ok {
			return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		length := int64(lst.Len())

		// Adjust negative indices
		if start < 0 {
			start = length + start
		}
		if stop < 0 {
			stop = length + stop
		}

		// Handle out of bounds indices
		if start < 0 {
			start = 0
		}
		if stop >= length {
			stop = length - 1
		}

		// If the start index is greater than the stop index, or the list is empty,
		// or the effective range is empty, the list is emptied.
		if start > stop || length == 0 || start >= length {
			s.data.Delete(key)
			return nil
		}

		// Remove elements before the start index
		for i := int64(0); i < start; i++ {
			if lst.Len() > 0 {
				lst.Remove(lst.Front())
			}
		}

		// Remove elements after the stop index
		for i := int64(0); i < length-(stop+1); i++ {
			if lst.Len() > 0 {
				lst.Remove(lst.Back())
			}
		}
		return nil
	}
	return nil // Key not found, no operation needed
}

// HSet sets the string value of a hash field.
// If the key does not exist, a new hash is created.
// If the field already exists in the hash, it is overwritten.
func (s *Storage) HSet(key, field, value string) (int64, error) {
	actual, _ := s.data.LoadOrStore(key, make(map[string]string))
	hash, ok := actual.(map[string]string)
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	_, fieldExists := hash[field]
	hash[field] = value

	if fieldExists {
		return 0, nil // Field already existed
	} else {
		return 1, nil // New field was set
	}
}

// HGet returns the value associated with field in the hash stored at key.
func (s *Storage) HGet(key, field string) (string, error) {
	if actual, ok := s.data.Load(key); ok {
		hash, ok := actual.(map[string]string)
		if !ok {
			return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if val, found := hash[field]; found {
			return val, nil
		}
		return "", nil // Field not found
	}
	return "", nil // Key not found
}

// HDel deletes one or more hash fields from the hash stored at key.
func (s *Storage) HDel(key string, fields ...string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		hash, ok := actual.(map[string]string)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		deletedCount := int64(0)
		for _, field := range fields {
			if _, found := hash[field]; found {
				delete(hash, field)
				deletedCount++
			}
		}
		// If the hash becomes empty, delete the key from main storage
		if len(hash) == 0 {
			s.data.Delete(key)
		}
		return deletedCount, nil
	}
	return 0, nil // Key not found, so no fields deleted
}

// HExists returns if field is an existing field in the hash stored at key.
func (s *Storage) HExists(key, field string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		hash, ok := actual.(map[string]string)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if _, found := hash[field]; found {
			return 1, nil
		}
		return 0, nil
	}
	return 0, nil // Key not found
}

// HLen returns the number of fields contained in the hash at key.
func (s *Storage) HLen(key string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		hash, ok := actual.(map[string]string)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return int64(len(hash)), nil
	}
	return 0, nil // Key not found, length is 0
}

// HGetAll returns all fields and values of the hash stored at key.
func (s *Storage) HGetAll(key string) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		hash, ok := actual.(map[string]string)
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		result := make([]string, 0, len(hash)*2)
		for field, value := range hash {
			result = append(result, field, value)
		}
		return result, nil
	}
	return []string{}, nil // Key not found, return empty list
}

// SAdd adds the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created with the specified members.
// If the key holds a value of another type, an error is returned.
func (s *Storage) SAdd(key string, members ...string) (int64, error) {
	actual, _ := s.data.LoadOrStore(key, make(map[string]struct{}))
	set, ok := actual.(map[string]struct{})
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	addedCount := int64(0)
	for _, member := range members {
		if _, found := set[member]; !found {
			set[member] = struct{}{}
			addedCount++
		}
	}
	return addedCount, nil
}

// SRem removes the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
// If the key holds a value of another type, an error is returned.
func (s *Storage) SRem(key string, members ...string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		set, ok := actual.(map[string]struct{})
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		removedCount := int64(0)
		for _, member := range members {
			if _, found := set[member]; found {
				delete(set, member)
				removedCount++
			}
		}
		// If the set becomes empty, delete the key from main storage
		if len(set) == 0 {
			s.data.Delete(key)
		}
		return removedCount, nil
	}
	return 0, nil // Key not found, so no members removed
}

// SIsMember returns if member is a member of the set stored at key.
func (s *Storage) SIsMember(key, member string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		set, ok := actual.(map[string]struct{})
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if _, found := set[member]; found {
			return 1, nil
		}
		return 0, nil
	}
	return 0, nil // Key not found, so member is not in set
}

// SCard returns the number of elements in the set stored at key.
func (s *Storage) SCard(key string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		set, ok := actual.(map[string]struct{})
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return int64(len(set)), nil
	}
	return 0, nil // Key not found, so set is empty
}

// SMembers returns all members of the set stored at key.
func (s *Storage) SMembers(key string) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		set, ok := actual.(map[string]struct{})
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		members := make([]string, 0, len(set))
		for member := range set {
			members = append(members, member)
		}
		return members, nil
	}
	return []string{}, nil // Key not found, return empty list
}

// SPop removes and returns a random member from the set value stored at key.
func (s *Storage) SPop(key string, count int64) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		set, ok := actual.(map[string]struct{})
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if len(set) == 0 {
			return []string{}, nil // Set is empty
		}

		members := make([]string, 0, len(set))
		for member := range set {
			members = append(members, member)
		}

		rand.Seed(time.Now().UnixNano())

		var popped []string
		numToPop := count
		if numToPop > int64(len(members)) || numToPop == 0 {
			numToPop = int64(len(members))
		}

		for i := int64(0); i < numToPop; i++ {
			randIndex := rand.Intn(len(members))
			poppedMember := members[randIndex]
			popped = append(popped, poppedMember)
			delete(set, poppedMember)

			// Remove from members slice to avoid re-picking
			members = append(members[:randIndex], members[randIndex+1:]...)
		}

		// If the set becomes empty, delete the key from main storage
		if len(set) == 0 {
			s.data.Delete(key)
		}

		return popped, nil
	}
	return []string{}, nil // Key not found, return empty list
}

// SRandMember returns a random member from the set value stored at key.
// If count is provided, returns an array of count random members.
// If count is positive, returns unique members.
// If count is negative, returns members that may be repeated.
func (s *Storage) SRandMember(key string, count int64) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		set, ok := actual.(map[string]struct{})
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if len(set) == 0 {
			return []string{}, nil // Set is empty
		}

		members := make([]string, 0, len(set))
		for member := range set {
			members = append(members, member)
		}

		rand.Seed(time.Now().UnixNano())

		var result []string
		if count == 0 {
			return []string{}, nil
		} else if count > 0 {
			// Return unique members
			numToReturn := count
			if numToReturn > int64(len(members)) {
				numToReturn = int64(len(members))
			}
			// Shuffle members and take the first numToReturn
			rand.Shuffle(len(members), func(i, j int) {
				members[i], members[j] = members[j], members[i]
			})
			result = members[:numToReturn]
		} else { // count < 0
			// Return members that may be repeated
			numToReturn := -count
			for i := int64(0); i < numToReturn; i++ {
				randIndex := rand.Intn(len(members))
				result = append(result, members[randIndex])
			}
		}
		return result, nil
	}
	return []string{}, nil // Key not found, return empty list
}

// SInter returns the members of the set resulting from the intersection of all the given sets.
func (s *Storage) SInter(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	// Get the first set
	actual, ok := s.data.Load(keys[0])
	if !ok {
		return []string{}, nil // First key not found, intersection is empty
	}
	set1, ok := actual.(map[string]struct{})
	if !ok {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Initialize intersection with the first set's members
	intersection := make(map[string]struct{})
	for member := range set1 {
		intersection[member] = struct{}{}
	}

	// Intersect with remaining sets
	for i := 1; i < len(keys); i++ {
		currentKey := keys[i]
		actual, ok := s.data.Load(currentKey)
		if !ok {
			return []string{}, nil // A key not found, intersection is empty
		}
		currentSet, ok := actual.(map[string]struct{})
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		newIntersection := make(map[string]struct{})
		for member := range intersection {
			if _, found := currentSet[member]; found {
				newIntersection[member] = struct{}{}
			}
		}
		intersection = newIntersection
		if len(intersection) == 0 {
			return []string{}, nil // Optimization: if intersection becomes empty, no need to continue
		}
	}

	result := make([]string, 0, len(intersection))
	for member := range intersection {
		result = append(result, member)
	}
	return result, nil
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (s *Storage) SUnion(keys ...string) ([]string, error) {
	unionSet := make(map[string]struct{})

	for _, key := range keys {
		if actual, ok := s.data.Load(key); ok {
			set, ok := actual.(map[string]struct{})
			if !ok {
				return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			for member := range set {
				unionSet[member] = struct{}{}
			}
		}
	}

	result := make([]string, 0, len(unionSet))
	for member := range unionSet {
		result = append(result, member)
	}
	return result, nil
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
func (s *Storage) SDiff(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	// Get the first set
	actual, ok := s.data.Load(keys[0])
	if !ok {
		return []string{}, nil // First key not found, difference is empty
	}
	set1, ok := actual.(map[string]struct{})
	if !ok {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Initialize difference with the first set's members
	difference := make(map[string]struct{})
	for member := range set1 {
		difference[member] = struct{}{}
	}

	// Remove members present in successive sets
	for i := 1; i < len(keys); i++ {
		currentKey := keys[i]
		actual, ok := s.data.Load(currentKey)
		if !ok {
			continue // If a key is not found, it's treated as an empty set, so no members to remove
		}
		currentSet, ok := actual.(map[string]struct{})
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		for member := range currentSet {
			delete(difference, member)
		}
	}

	result := make([]string, 0, len(difference))
	for member := range difference {
		result = append(result, member)
	}
	return result, nil
}

// ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
// If a member is already a member of the sorted set, its score is updated, and the element is reinserted
// at the correct position to ensure the correct ordering.
func (s *Storage) ZAdd(key string, members ...ZSetMember) (int64, error) {
	actual, _ := s.data.LoadOrStore(key, make(map[string]ZSetMember))
	zset, ok := actual.(map[string]ZSetMember)
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	addedCount := int64(0)
	for _, member := range members {
		if existingMember, found := zset[member.Member]; !found || existingMember.Score != member.Score {
			zset[member.Member] = member
			addedCount++
		}
	}
	return addedCount, nil
}

// ZScore returns the score of member in the sorted set at key.
// If member does not exist in the sorted set, or key does not exist, nil is returned.
func (s *Storage) ZScore(key, member string) (float64, bool, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if zMember, found := zset[member]; found {
			return zMember.Score, true, nil
		}
		return 0, false, nil // Member not found
	}
	return 0, false, nil // Key not found
}

// ZRem removes the specified members from the sorted set stored at key.
// Non existing members are ignored.
// If key does not exist, it is treated as an empty sorted set and this command returns 0.
// If the key holds a value of another type, an error is returned.
func (s *Storage) ZRem(key string, members ...string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		removedCount := int64(0)
		for _, member := range members {
			if _, found := zset[member]; found {
				delete(zset, member)
				removedCount++
			}
		}
		// If the sorted set becomes empty, delete the key from main storage
		if len(zset) == 0 {
			s.data.Delete(key)
		}
		return removedCount, nil
	}
	return 0, nil // Key not found, so no members removed
}

// ZCard returns the number of elements in the sorted set at key.
func (s *Storage) ZCard(key string) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return int64(len(zset)), nil
	}
	return 0, nil // Key not found, so sorted set is empty
}

// ZRange returns a range of members from a sorted set.
// The range is specified by start and stop indexes (0-based).
// WithScores option includes scores in the reply.
func (s *Storage) ZRange(key string, start, stop int64, withScores bool) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if len(zset) == 0 {
			return []string{}, nil
		}

		// Convert map to slice for sorting
		members := make([]ZSetMember, 0, len(zset))
		for _, member := range zset {
			members = append(members, member)
		}

		// Sort by score, then by member string for ties
		sort.Slice(members, func(i, j int) bool {
			if members[i].Score != members[j].Score {
				return members[i].Score < members[j].Score
			}
			return members[i].Member < members[j].Member
		})

		length := int64(len(members))

		// Adjust negative indices
		if start < 0 {
			start = length + start
		}
		if stop < 0 {
			stop = length + stop
		}

		// Handle out of bounds indices
		if start < 0 {
			start = 0
		}
		if stop >= length {
			stop = length - 1
		}

		if start > stop || length == 0 {
			return []string{}, nil // Empty list or invalid range
		}

		var result []string
		for i := start; i <= stop; i++ {
			result = append(result, members[i].Member)
			if withScores {
				result = append(result, strconv.FormatFloat(members[i].Score, 'f', -1, 64))
			}
		}
		return result, nil
	}
	return []string{}, nil // Key not found, return empty list
}

// ZRangeByScore returns all the elements in the sorted set at key with a score between min and max (inclusive).
// The elements are considered to be ordered from low to high scores.
// Options for LIMIT offset count and WITHSCORES are supported.
func (s *Storage) ZRangeByScore(key string, min, max float64, offset, count int64, withScores bool) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		var filteredMembers []ZSetMember
		for _, member := range zset {
			if member.Score >= min && member.Score <= max {
				filteredMembers = append(filteredMembers, member)
			}
		}

		// Sort by score, then by member string for ties
		sort.Slice(filteredMembers, func(i, j int) bool {
			if filteredMembers[i].Score != filteredMembers[j].Score {
				return filteredMembers[i].Score < filteredMembers[j].Score
			}
			return filteredMembers[i].Member < filteredMembers[j].Member
		})

		var result []string
		startIndex := offset
		if startIndex < 0 {
			startIndex = 0
		}

		endIndex := startIndex + count
		if count == -1 || endIndex > int64(len(filteredMembers)) {
			endIndex = int64(len(filteredMembers))
		}

		for i := startIndex; i < endIndex; i++ {
			result = append(result, filteredMembers[i].Member)
			if withScores {
				result = append(result, strconv.FormatFloat(filteredMembers[i].Score, 'f', -1, 64))
			}
		}
		return result, nil
	}
	return []string{}, nil // Key not found, return empty list
}

// ZCount returns the number of elements in the sorted set at key with a score between min and max (inclusive).
func (s *Storage) ZCount(key string, min, max float64) (int64, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		count := int64(0)
		for _, member := range zset {
			if member.Score >= min && member.Score <= max {
				count++
			}
		}
		return count, nil
	}
	return 0, nil // Key not found, count is 0
}

// ZIncrBy increments the score of member in the sorted set at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (a new sorted set if key does not exist).
// If the key holds a value of another type, an error is returned.
func (s *Storage) ZIncrBy(key string, increment float64, member string) (float64, error) {
	actual, _ := s.data.LoadOrStore(key, make(map[string]ZSetMember))
	zset, ok := actual.(map[string]ZSetMember)
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	currentMember, found := zset[member]
	newScore := increment
	if found {
		newScore = currentMember.Score + increment
	}
	zset[member] = ZSetMember{Member: member, Score: newScore}
	return newScore, nil
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, so the member with the lowest score has rank 0.
// If member does not exist in the sorted set, nil is returned.
func (s *Storage) ZRank(key, member string) (int64, bool, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		// Check if member exists
		if _, found := zset[member]; !found {
			return 0, false, nil
		}

		// Convert map to slice for sorting
		members := make([]ZSetMember, 0, len(zset))
		for _, m := range zset {
			members = append(members, m)
		}

		// Sort by score, then by member string for ties
		sort.Slice(members, func(i, j int) bool {
			if members[i].Score != members[j].Score {
				return members[i].Score < members[j].Score
			}
			return members[i].Member < members[j].Member
		})

		for i, m := range members {
			if m.Member == member {
				return int64(i), true, nil
			}
		}
	}
	return 0, false, nil // Should not reach here if member was found initially
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, so the member with the highest score has rank 0.
// If member does not exist in the sorted set, nil is returned.
func (s *Storage) ZRevRank(key, member string) (int64, bool, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		// Check if member exists
		if _, found := zset[member]; !found {
			return 0, false, nil
		}

		// Convert map to slice for sorting
		members := make([]ZSetMember, 0, len(zset))
		for _, m := range zset {
			members = append(members, m)
		}

		// Sort by score in descending order, then by member string for ties
		sort.Slice(members, func(i, j int) bool {
			if members[i].Score != members[j].Score {
				return members[i].Score > members[j].Score // Descending order
			}
			return members[i].Member < members[j].Member // Ascending for ties
		})

		for i, m := range members {
			if m.Member == member {
				return int64(i), true, nil
			}
		}
	}
	return 0, false, nil // Should not reach here if member was found initially
}

// ZRevRange returns a range of members from a sorted set, ordered from high to low scores.
// The range is specified by start and stop indexes (0-based).
// WithScores option includes scores in the reply.
func (s *Storage) ZRevRange(key string, start, stop int64, withScores bool) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if len(zset) == 0 {
			return []string{}, nil
		}

		// Convert map to slice for sorting
		members := make([]ZSetMember, 0, len(zset))
		for _, member := range zset {
			members = append(members, member)
		}

		// Sort by score in descending order, then by member string for ties
		sort.Slice(members, func(i, j int) bool {
			if members[i].Score != members[j].Score {
				return members[i].Score > members[j].Score
			}
			return members[i].Member < members[j].Member
		})

		length := int64(len(members))

		// Adjust negative indices
		if start < 0 {
			start = length + start
		}
		if stop < 0 {
			stop = length + stop
		}

		// Handle out of bounds indices
		if start < 0 {
			start = 0
		}
		if stop >= length {
			stop = length - 1
		}

		if start > stop || length == 0 {
			return []string{}, nil // Empty list or invalid range
		}

		var result []string
		for i := start; i <= stop; i++ {
			result = append(result, members[i].Member)
			if withScores {
				result = append(result, strconv.FormatFloat(members[i].Score, 'f', -1, 64))
			}
		}
		return result, nil
	}
	return []string{}, nil // Key not found, return empty list
}

// ZRevRangeByScore returns all the elements in the sorted set at key with a score between max and min (inclusive).
// The elements are considered to be ordered from high to low scores.
// Options for LIMIT offset count and WITHSCORES are supported.
func (s *Storage) ZRevRangeByScore(key string, max, min float64, offset, count int64, withScores bool) ([]string, error) {
	if actual, ok := s.data.Load(key); ok {
		zset, ok := actual.(map[string]ZSetMember)
		if !ok {
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		var filteredMembers []ZSetMember
		for _, member := range zset {
			if member.Score <= max && member.Score >= min {
				filteredMembers = append(filteredMembers, member)
			}
		}

		// Sort by score in descending order, then by member string for ties
		sort.Slice(filteredMembers, func(i, j int) bool {
			if filteredMembers[i].Score != filteredMembers[j].Score {
				return filteredMembers[i].Score > filteredMembers[j].Score
			}
			return filteredMembers[i].Member < filteredMembers[j].Member
		})

		var result []string
		startIndex := offset
		if startIndex < 0 {
			startIndex = 0
		}

		endIndex := startIndex + count
		if count == -1 || endIndex > int64(len(filteredMembers)) {
			endIndex = int64(len(filteredMembers))
		}

		for i := startIndex; i < endIndex; i++ {
			result = append(result, filteredMembers[i].Member)
			if withScores {
				result = append(result, strconv.FormatFloat(filteredMembers[i].Score, 'f', -1, 64))
			}
		}
		return result, nil
	}
	return []string{}, nil // Key not found, return empty list
}