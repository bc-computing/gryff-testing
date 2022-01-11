package state

import (
	"log"
)

type Operation uint8

const (
	NONE Operation = iota
	PUT
	GET
	DELETE
	RLOCK
	WLOCK
	CAS
)

type Value int64

const NIL Value = 0

type Key int64

type Command struct {
	Op       Operation
	K        Key
	V        Value
	K1       Key
	V1       Value
	K2       Key
	V2       Value
	K3       Key
	V3       Value
	K4       Key
	V4       Value
	K5       Key
	V5       Value
	K6       Key
	V6       Value
	K7       Key
	V7       Value
	K8       Key
	V8       Value
	K9       Key
	V9       Value
	K10      Key
	V10      Value
	K11      Key
	V11      Value
	K12      Key
	V12      Value
	K13      Key
	V13      Value
	K14      Key
	V14      Value
	K15      Key
	V15      Value
	K16      Key
	V16      Value
	K17      Key
	V17      Value
	K18      Key
	V18      Value
	K19      Key
	V19      Value
	K20      Key
	V20      Value
	K21      Key
	V21      Value
	K22      Key
	V22      Value
	K23      Key
	V23      Value
	K24      Key
	V24      Value
	K25      Key
	V25      Value
	K26      Key
	V26      Value
	K27      Key
	V27      Value
	K28      Key
	V28      Value
	K29      Key
	V29      Value
	K30      Key
	V30      Value
	K31      Key
	V31      Value
	K32      Key
	V32      Value
	K33      Key
	V33      Value
	K34      Key
	V34      Value
	K35      Key
	V35      Value
	K36      Key
	V36      Value
	K37      Key
	V37      Value
	K38      Key
	V38      Value
	K39      Key
	V39      Value
	K40      Key
	V40      Value
	K41      Key
	V41      Value
	K42      Key
	V42      Value
	K43      Key
	V43      Value
	K44      Key
	V44      Value
	K45      Key
	V45      Value
	K46      Key
	V46      Value
	K47      Key
	V47      Value
	K48      Key
	V48      Value
	K49      Key
	V49      Value
	K50      Key
	V50      Value
	K51      Key
	V51      Value
	K52      Key
	V52      Value
	K53      Key
	V53      Value
	K54      Key
	V54      Value
	K55      Key
	V55      Value
	K56      Key
	V56      Value
	K57      Key
	V57      Value
	K58      Key
	V58      Value
	K59      Key
	V59      Value
	K60      Key
	V60      Value
	K61      Key
	V61      Value
	K62      Key
	V62      Value
	OldValue Value
}

type State struct {
	Store map[Key]Value
	//DB *leveldb.DB
}

func NewState() *State {
	/*
		 d, err := leveldb.Open("/Users/iulian/git/epaxos-batching/dpaxos/bin/db", nil)

		 if err != nil {
				 log.Printf("Leveldb open failed: %v\n", err)
		 }

		 return &State{d}
	*/

	return &State{make(map[Key]Value)}
}

func AllOpTypes() []Operation {
	return []Operation{PUT, GET, CAS}
}

func GetConflictingOpTypes(op Operation) []Operation {
	switch op {
	case PUT:
		return []Operation{PUT, GET, CAS}
	case GET:
		return []Operation{PUT, GET, CAS}
	case CAS:
		return []Operation{PUT, GET, CAS}
	default:
		log.Fatalf("Unsupported op type: %d.\n", op)
		return nil
	}
}

func OpTypesConflict(op1 Operation, op2 Operation) bool {
	return op1 == PUT || op1 == CAS || op2 == PUT || op2 == CAS
}

func Conflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == PUT || gamma.Op == CAS || delta.Op == PUT || delta.Op == CAS {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

func (command *Command) CanReplyWithoutExecute() bool {
	return command.Op == PUT
}

func IsRead(command *Command) bool {
	return command.Op == GET
}

func (c *Command) Execute(st *State) Value {
	//log.Printf("Executing (%d, %d)\n", c.K, c.V)

	//var key, value [8]byte

	//    st.mutex.Lock()
	//    defer st.mutex.Unlock()

	switch c.Op {
	case PUT:
		/*
		 binary.LittleEndian.PutUint64(key[:], uint64(c.K))
		 binary.LittleEndian.PutUint64(value[:], uint64(c.V))
		 st.DB.Set(key[:], value[:], nil)
		*/

		st.Store[c.K] = c.V
		st.Store[c.K1] = c.V1
		st.Store[c.K2] = c.V2
		st.Store[c.K3] = c.V3
		st.Store[c.K4] = c.V4
		st.Store[c.K5] = c.V5
		st.Store[c.K6] = c.V6
		st.Store[c.K7] = c.V7
		st.Store[c.K8] = c.V8
		st.Store[c.K9] = c.V9
		st.Store[c.K10] = c.V10
		st.Store[c.K11] = c.V11
		st.Store[c.K12] = c.V12
		st.Store[c.K13] = c.V13
		st.Store[c.K14] = c.V14
		st.Store[c.K15] = c.V15
		st.Store[c.K16] = c.V16
		st.Store[c.K17] = c.V17
		st.Store[c.K18] = c.V18
		st.Store[c.K19] = c.V19
		st.Store[c.K20] = c.V20
		st.Store[c.K21] = c.V21
		st.Store[c.K22] = c.V22
		st.Store[c.K23] = c.V23
		st.Store[c.K24] = c.V24
		st.Store[c.K25] = c.V25
		st.Store[c.K26] = c.V26
		st.Store[c.K27] = c.V27
		st.Store[c.K28] = c.V28
		st.Store[c.K29] = c.V29
		st.Store[c.K30] = c.V30
		st.Store[c.K31] = c.V31
		st.Store[c.K32] = c.V32
		st.Store[c.K33] = c.V33
		st.Store[c.K34] = c.V34
		st.Store[c.K35] = c.V35
		st.Store[c.K36] = c.V36
		st.Store[c.K37] = c.V37
		st.Store[c.K38] = c.V38
		st.Store[c.K39] = c.V39
		st.Store[c.K40] = c.V40
		st.Store[c.K41] = c.V41
		st.Store[c.K42] = c.V42
		st.Store[c.K43] = c.V43
		st.Store[c.K44] = c.V44
		st.Store[c.K45] = c.V45
		st.Store[c.K46] = c.V46
		st.Store[c.K47] = c.V47
		st.Store[c.K48] = c.V48
		st.Store[c.K49] = c.V49
		st.Store[c.K50] = c.V50
		st.Store[c.K51] = c.V51
		st.Store[c.K52] = c.V52
		st.Store[c.K53] = c.V53
		st.Store[c.K54] = c.V54
		st.Store[c.K55] = c.V55
		st.Store[c.K56] = c.V56
		st.Store[c.K57] = c.V57
		st.Store[c.K58] = c.V58
		st.Store[c.K59] = c.V59
		st.Store[c.K60] = c.V60
		st.Store[c.K61] = c.V61
		st.Store[c.K62] = c.V62
		return c.V

	case GET:
		if val, present := st.Store[c.K]; present {
			return val
		}
	case CAS:
		if val, present := st.Store[c.K]; present {
			if val == c.OldValue {
				st.Store[c.K] = c.V
				return val
			}
		}
	}

	return NIL
}
