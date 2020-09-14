package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Role int

const (
	None      Role = 0
	Leader    Role = 1
	Candidate Role = 2
	Follower  Role = 3
)

type Log struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role Role

	/****** Persistent State ******/

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int //candidateId that received vote in current term (or null if none)
	votedTerm   int
	/*
		log entries; each entry contains command for state machine,
		and term when entry was received by leader (first index is 1)
	*/
	log []Log

	/****** Persistent State ******/

	/****** Volatile state on all servers ******/
	commitIndex int //index of highest log entry known to be
	committed   int //(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	/****** Volatile state on all servers ******/

	/****** Volatile state on all leaders (Reinitialized after election) ******/
	/*
		for each server, index of the next log entry
		to send to that server (initialized to leader
		last log index + 1)
	*/
	nextIndex []int
	/*
		for each server, index of highest log entry
		known to be replicated on server
		(initialized to 0, increases monotonically)
	*/
	matchIndex []int
	/****** Volatile state on all leaders (Reinitialized after election)******/

	lastHeartbeat int64 // nano
	lastVoteTime  int64 // nano
	applyCh       chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	/*Arguments:*/
	Term         int // candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	/*Results:*/
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
/**
Receiver implementation:
	1. Reply false if term < currentTerm (§5.1)
	2. If votedFor is null or candidateId, and candidate’s log is at
	least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	DPrintf("start RequestVote: server: %d args.Term: %d, rf.votedTerm: %d rf.votedFor: %d, args.CandidateId: %d, args.LastLogIndex: %d, rf.committed: %d", rf.me, args.Term, rf.votedTerm, rf.votedFor, args.CandidateId, args.LastLogIndex, rf.committed)
	if args.Term > rf.votedTerm {
		if args.LastLogIndex >= rf.committed {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.votedTerm = args.Term
			rf.currentTerm = args.Term
			rf.lastVoteTime = time.Now().UnixNano()

			DPrintf("end RequestVote: server: %d args.Term: %d, rf.votedTerm: %d rf.votedFor: %d, rf.CandidateId: %d", rf.me, args.Term, rf.votedTerm, rf.votedFor, args.CandidateId)
			return
		}
	} else if args.Term == rf.votedTerm {
		if rf.votedFor == args.CandidateId {
			rf.lastVoteTime = time.Now().UnixNano()
			reply.VoteGranted = true
			DPrintf("end RequestVote: server: %d args.Term: %d, rf.votedTerm: %d rf.votedFor: %d, rf.CandidateId: %d", rf.me, args.Term, rf.votedTerm, rf.votedFor, args.CandidateId)
			return
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("end RequestVote: server: %d args.Term: %d, rf.votedTerm: %d rf.votedFor: %d, rf.CandidateId: %d", rf.me, args.Term, rf.votedTerm, rf.votedFor, args.CandidateId)
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := rf.committed
	term := rf.currentTerm
	isLeader := rf.role == Leader

	// Your code here (2B).
	if !isLeader {
		return index, term, false
	}
	var prevLogIndex int
	var prevLogTerm int
	if rf.commitIndex == 1 {
		prevLogIndex = 0
		prevLogTerm = 0
	} else {
		prevLogIndex = rf.commitIndex - 1
		prevLogTerm = rf.log[rf.commitIndex-1].Term
	}
	rf.log = append(rf.log, Log{
		Term:    rf.currentTerm,
		Command: command,
	})
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		var reply AppendEntryReply
		rf.sendAppendEntries(i, &AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderIndex:  rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries: []Log{{
				Term:    rf.currentTerm,
				Command: command,
			}},
			LeaderCommit: rf.commitIndex,
		}, &reply)
	}
	rf.committed = rf.commitIndex
	rf.commitIndex = rf.commitIndex + 1
	rf.applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: rf.committed,
	}
	return rf.committed, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return milliseconds
func (rf *Raft) getCandidateRandomTime() int {
	rand.Seed(time.Now().UnixNano())
	return 150 + rand.Intn(150+1)
}

type AppendEntryArgs struct {
	Term         int   // leader’s term
	LeaderIndex  int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntryReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

/**
Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
*/
/**
Receiver implementation:
	1. Reply false if term < currentTerm (§5.1)
	2. Reply false if log doesn’t contain an entry at prevLogIndex
	whose term matches prevLogTerm (§5.3)
	3. If an existing entry conflicts with a new one (same index
	but different terms), delete the existing entry and all that
	follow it (§5.3)
	4. Append any new entries not already in the log
	5. If leaderCommit > commitIndex, set commitIndex =
	min(leaderCommit, index of last new entry)
*/
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	reply.Term = rf.currentTerm
	if len(args.Entries) == 0 { // a heartbeat
		reply.Success = true

		rf.lastHeartbeat = time.Now().UnixNano()

		if args.Term >= rf.currentTerm {
			if rf.role != Follower {
				rf.beFollower()
			}
		}

		rf.currentTerm = args.Term
		return
	} else { // save command
		if args.Term < rf.currentTerm {
			reply.Success = false
			return
		}
		if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
		rf.log = append(rf.log, args.Entries...)
		for i, entry := range args.Entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: args.PrevLogIndex + 1 + i,
			}
		}
		rf.committed = args.PrevLogIndex + 1 + len(args.Entries)
	}
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.applyCh = applyCh

	rf.log = append(rf.log, Log{})
	rf.commitIndex = 1
	rf.committed = 0

	// Your initialization code here (2A, 2B, 2C).

	// heartbeat
	go func() {
		sendHeartbeat(rf)
	}()

	rf.beFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func sendHeartbeat(rf *Raft) {

	go func() {
		for {

			time.Sleep(150 * time.Millisecond)

			if rf.role == Follower && time.Now().UnixNano()/1e6-rf.lastHeartbeat/1e6 > 300 {
				time.Sleep(time.Duration(rf.getCandidateRandomTime()) * time.Millisecond)
				rf.beCandidate()
			}
		}
	}()

	for {
		go doSendHeartbeat(rf)
		time.Sleep(time.Duration(rf.getHeartbeatTime()) * time.Millisecond)
	}
}

func doSendHeartbeat(rf *Raft) {
	if _, isLeader := rf.GetState(); isLeader {
		shouldStepDown := false
		peerCount := 0
		var mutex sync.Mutex

		var wg sync.WaitGroup

		for i, _ := range rf.peers {
			if i == rf.me {
				peerCount++
				continue
			}
			wg.Add(1)
			go func(i int, peer *labrpc.ClientEnd) {
				defer wg.Done()

				var reply AppendEntryReply
				ok := peer.Call("Raft.AppendEntries", &AppendEntryArgs{
					Term: rf.currentTerm,
				}, &reply)
				if ok {
					mutex.Lock()
					peerCount++
					mutex.Unlock()
				}
				if reply.Term > rf.currentTerm {
					shouldStepDown = true
				}
			}(i, rf.peers[i])
		}
		wg.Wait()
		if peerCount < len(rf.peers)/2+1 {
			shouldStepDown = true
		}
		if shouldStepDown {
			rf.beFollower()
			time.Sleep(time.Duration(rf.getCandidateRandomTime()) * time.Millisecond)
			rf.beCandidate()
		}
	}

}

func (rf *Raft) beCandidate() {
	if rf.role != Follower {
		return
	}
	for {
		if time.Now().UnixNano()/1e6-rf.lastVoteTime/1e6 < 150 {
			time.Sleep(time.Duration(rf.getCandidateRandomTime()) * time.Millisecond)
			continue
		}
		DPrintf("now: %d, can be Candidate: %d, term: %d, rf.lastHeartbeat: %d, cost: %d", time.Now().UnixNano()/1e6, rf.me, rf.currentTerm, rf.lastHeartbeat, time.Now().UnixNano()/1e6-rf.lastHeartbeat/1e6)
		if time.Now().UnixNano()/1e6-rf.lastHeartbeat/1e6 < rf.getHeartbeatTime() {
			return
		}
		rf.role = Candidate
		DPrintf("now: %d, be Candidate: %d, term: %d", time.Now().UnixNano()/1e6, rf.me, rf.currentTerm)

		rf.votedFor = rf.me
		rf.currentTerm++
		rf.votedTerm = rf.currentTerm
		//rf.lastVoteTime = time.Now().UnixNano()

		peerCount := 0
		agreeCount := 1
		var mutex sync.Mutex
		var wg sync.WaitGroup
		for i, _ := range rf.peers {
			if i == rf.me {
				peerCount++
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ctx := context.Background()
				done := make(chan struct{}, 1)
				go func(ctx context.Context) {
					var reply RequestVoteReply
					ok := rf.sendRequestVote(i, &RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: rf.commitIndex,
						LastLogTerm:  rf.currentTerm,
					}, &reply)
					if ok {
						mutex.Lock()
						peerCount++
						if reply.VoteGranted {
							agreeCount++
						} else if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
						}
						mutex.Unlock()
					}
					done <- struct{}{}
				}(ctx)

				select {
				case <-done:
					return
				case <-time.After(100 * time.Millisecond):
					return
				}

			}(i)
		}
		wg.Wait()
		DPrintf("server: %d, agreeCount: %d", rf.me, agreeCount)
		if agreeCount >= len(rf.peers)/2+1 {
			rf.beLeader()
			doSendHeartbeat(rf)
			break
		}
		time.Sleep(time.Duration(rf.getCandidateRandomTime()) * time.Millisecond)
	}
}

func (rf *Raft) getHeartbeatTime() int64 {
	return 75
}

func (rf *Raft) beFollower() {
	DPrintf("be Follower: %d, term: %d, rf.lastHeartbeat: %d", rf.me, rf.currentTerm, rf.lastHeartbeat)
	rf.role = Follower
}

func (rf *Raft) beLeader() {
	rf.role = Leader
	DPrintf("be leader: %d, term: %d", rf.me, rf.currentTerm)
}
