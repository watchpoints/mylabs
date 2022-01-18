// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64

	// leader 和 follower 状态同步是异步的，leader 将日志发送给 follower，follower 再给回复；
	// Match 标识经过 Follower 确认接收的最大日志索引（注意了，这个跟 raft commit 里面的是不一样的，这个仅仅是发往节点的日志）
	// Next 标识下一次发送日志的开始索引
	// Next - Match - 1 说明的就是发送了，但是还没有收到 follower 确认的日志数量
}

type Raft struct {
	id uint64
	// 任期编号
	Term uint64

	//选举了谁当 leader（投票给了谁）
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64 //小王疑问：这是什么意思

	// isLearner is true if the local raft node is a learner.
	isLearner bool
	step      stepFunc
}

type stepFunc func(r *Raft, m pb.Message) error

// newRaft return a raft peer with the given config
// raft 状态机
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	raftlog := newLogWithSize(c.Storage)
	r := &Raft{
		id:               c.ID,
		Term:             0,
		isLearner:        false,
		RaftLog:          raftlog,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool, len(c.peers)),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}

	if len(c.peers) > 0 {
		for index, id := range c.peers {
			log.Println("read conf.peers =", index, id)
			var pr Progress
			r.Prs[id] = &pr
		}
	}
	// 从持久化的 cs 数据中恢复集群配置
	//cfg, prs, err := confchange.Restore(confchange.Changer{
	//	LastIndex: raftlog.LastIndex(),
	//}, cs)
	//if err != nil {
	//	panic(err)
	//}
	// 初始化集群配置 switchToConfig
	//assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	// 加载出持久化了的 raft 的状态，比如 term，vote，commit index 等
	//if !IsEmptyHardState(hs) {
	//r.loadState(hs)
	//}

	//读取配置文件信息
	if c.Applied > 0 {
		r.RaftLog.AppliedTo(c.Applied)
	}

	//集群节点启动时候，默认都是StateFollower状态
	r.becomeFollower(r.Term, None)

	return r
}

// leader 发送消息到 mailbox（之后会作为 mailbox 输出出去，业务拿到之后的处理是发送网络）
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	// 1 暂存本地日志信息 2 网络发出
	return r.maybeSendAppend(to, true)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	//r.tick()
	//业务逻辑：周期性触发动作
	//https://asktug.com/t/topic/274755/2
	switch r.State {
	case StateLeader:
		//tikc任务 发送心跳包
		//tickHeartbeat
		{
			r.heartbeatElapsed++ // only leader keeps heartbeatElapsed.
			//r.electionElapsed++  //term
			if r.heartbeatElapsed < r.heartbeatTimeout {
				return
			}
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				m := pb.Message{} //本地构造待发送心跳包
				m.MsgType = pb.MessageType_MsgHeartbeat
				m.From = r.id
				m.To = id
				m.Term = r.Term
				m.Commit = r.RaftLog.committed
				r.msgs = append(r.msgs, m)
				log.Println("raft.tick() write local data =", id, m)
			}
		}
	case StateFollower:
		{
			//follower节点的规则：https://asktug.com/t/topic/69001
			//step01 -心跳超时触发选举
			r.heartbeatElapsed++
			//log.Println("raft.tick() StateFollower heartbeatElapsed=", r.heartbeatElapsed)
			if r.heartbeatElapsed >= r.electionTimeout {
				r.becomeCandidate()
				r.heartbeatElapsed = 0
				r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id, Term: r.Term})
				log.Println("StateFollower-->tickElection")
			}
		}
	case StateCandidate:
		//step01 -选举超时
		r.electionElapsed++
		//log.Println("raft.tick() StateCandidate electionElapsed=", r.electionElapsed)
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id, Term: r.Term})
			//tickElection
		}

	default:
		log.Println(" err unkown tick")
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	r.step = StepFollower
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	// StateLeader -->StateFollower
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	//step01 任期+1
	r.reset(r.Term + 1)
	log.Println("Candidate term =", r.Term)
	//step02  自己投票自己
	r.Vote = r.id
	r.votes[r.id] = true
	//bug1 panic: assignment to entry in nil map
	//https://github.com/kevinyan815/gocookbook/issues/7

	//step03  改变状态
	r.State = StateCandidate
	//step04  业务回调
	r.step = StepCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	//step01 状态基改变方向 StateFollower -->StateCandidate -->StateLeader
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}

	//step02 从StateCandidate -->StateLeader 任期从StateCandidate
	r.reset(r.Term)
	r.Lead = r.id         //更新leaderid
	r.State = StateLeader //更新状态

	// 小王疑问：Followers prs 该怎么处理 ？这个不清楚
	//r.Prs.Progress[r.id].BecomeReplicate()

	// 成为 leader 的第一件事，发一个 empty entry 广播出去。
	// 添加一条空白消息，会消耗一个 index

	emptyEnt := pb.Entry{Data: nil}
	if !r.AppendEntry(&emptyEnt) {
		log.Println("Empty entry failed")
	}

	log.Printf("becomeLeader:id =%x became leader at term =%d", r.id, r.Term)

	r.step = StepLeader
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled

func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Printf("r.State =%d id= %x [term: %d] received a %s message ", r.State,
		r.id, r.Term, m.MsgType)
	//step01 处理请求term
	switch r.State {
	case StateFollower:
		//任期比自己大处理策略
		if m.Term > r.Term {
			r.Term = m.Term
		} else if m.Term == r.Term {
			// 处理业务
		} else {
			log.Printf(" err StateFollower  msg  term %d,less then cur term  %d ", m.Term, r.Term)
			return errors.New("less then cur term")
		}
	case StateCandidate:
		//任期比自己大处理策略
		//AppendEntries RPC
		if m.Term > r.Term {
			//step 01 update  Term
			r.Term = m.Term
			//step 02 becomeFollower
			r.becomeFollower(r.Term, m.From)

		} else if m.Term == r.Term {
			// 处理业务
		} else {
			log.Printf(" err  StateCandidate msg  term %d,less then cur term  %d ", m.Term, r.Term)
			return errors.New("less then cur term")
		}

	case StateLeader:
		//任期比自己大处理策略
		if m.Term > r.Term {
			//step 01 update  Term
			r.Term = m.Term

			//step 02 becomeFollower
			switch {
			case m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgBeat:
				r.becomeFollower(m.Term, m.From)
				log.Printf(" StateLeader %x [term: %d] received a %s message with higher term from %x [term: %d]",
					r.id, r.Term, m.MsgType, m.From, m.Term)
			default:
				return errors.New("unkown MsgType")
			}
		} else if m.Term == r.Term {
			// 处理业务
		} else {
			log.Printf(" err StateLeader  msg  term %d,less then cur term  %d ", m.Term, r.Term)
			return errors.New("less then cur term")
		}
	}
	//ste02 处理请求类型
	//The raft.Raft.Step() is the entrance of message handling,
	//you should handle messages like MsgRequestVote, MsgHeartbeat and their response.
	switch m.MsgType {
	//91 tick触发领导选举 tick--MessageType_MsgHup
	//02 发送：MessageType_MsgHup 请求 peers 处理MessageType_MsgRequestVote msg
	case pb.MessageType_MsgHup:
		//Leader election

		//01-leader状态根本不会选举
		if r.State == StateLeader {
			return errors.New("MsgHup failed")
		}
		//02-该消息仅供本地使用
		if m.From != m.To {
			return errors.New("MsgHup failed")
		}
		if r.State == StateFollower {
			r.becomeCandidate()
		}

		for id := range r.Prs {
			if id == r.id {
				if len(r.Prs) == 1 {
					log.Println(" MessageType_MsgHup -->becomeCandidate-->becomeLeader ")
					r.becomeLeader() //只有个一个节点：未知情况
				}
				continue
			}
			m := pb.Message{} //Leader election
			m.MsgType = pb.MessageType_MsgRequestVote
			m.From = r.id
			m.To = id
			m.Term = r.Term
			r.msgs = append(r.msgs, m)
			//log.Println("raft.tick() Leader election req ", m)

		}
	//03 peers解析msg MessageType_MsgRequestVote
	//04 回应 peers delMessageType_MsgRequestVote
	// Follower||?处理投票逻 RequestVote RPC
	case pb.MessageType_MsgRequestVote:

		//只投递一次原则
		//1 Term 不小于 同意
		//2. 我已经投递过a。如果新请求在来。我不同意的。【只投递一次，需要持久化】
		//2. 我已经投递过a，因为网络其他原因重复发送 同意
		//3  日志和日志term不小于 这里没有 3 之前没投递过 同意

		//01  请求的term 不能小于( m.Term可以大于等于)
		if r.Term > m.Term {

			Votem := pb.Message{} //Leader election
			Votem.MsgType = pb.MessageType_MsgRequestVoteResponse
			Votem.From = r.id
			Votem.To = m.From
			Votem.Term = r.Term
			Votem.Reject = true //投反对票
			r.msgs = append(r.msgs, Votem)
			return ErrProposalDropped
		}

		if r.Vote != None && r.Vote != m.From {
			Votem := pb.Message{} //Leader election
			Votem.MsgType = pb.MessageType_MsgRequestVoteResponse
			Votem.From = r.id
			Votem.To = m.From
			Votem.Term = r.Term
			Votem.Reject = true //投反对票
			r.msgs = append(r.msgs, Votem)
			return ErrProposalDropped
		}

		//03 日志 m.LogTerm >lastTerm ||(m.LogTerm == lastTerm && m.Index >= lastIndex)
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err1 := r.RaftLog.Term(lastIndex)

		if lastIndex > 0 && m.Index > 0 && err1 == nil {
			if lastTerm > m.LogTerm || (lastTerm == m.LogTerm && lastIndex > m.Index) {
				Votem := pb.Message{} //Leader election
				Votem.MsgType = pb.MessageType_MsgRequestVoteResponse
				Votem.From = r.id
				Votem.To = m.From
				Votem.Term = r.Term
				Votem.Reject = true //投反对票
				r.msgs = append(r.msgs, Votem)
				return ErrProposalDropped
			}
		}

		//04 同意
		//状态改变
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}

		//投同意票
		Votem := pb.Message{} //Leader election
		Votem.MsgType = pb.MessageType_MsgRequestVoteResponse
		Votem.From = r.id
		Votem.To = m.From
		Votem.Term = r.Term
		Votem.Reject = false
		r.msgs = append(r.msgs, Votem)

	default:
		err := r.step(r, m)
		if err != nil {
			return err
		}

	}

	return nil
}

func StepLeader(r *Raft, m pb.Message) error {
	log.Printf("StepLeader begin ...")
	switch m.MsgType {

	case pb.MessageType_MsgPropose:
		//step1 check log and id
		if len(m.Entries) == 0 {
			log.Panic("stepped empty MsgProp")
		}
		if _, ok := r.Prs[r.id]; !ok {
			return ErrProposalDropped
		}

		//step 02 Leader AppendEntries RPC

		if !r.AppendEntry(m.Entries...) {
			return ErrProposalDropped
		}
		// 把消息发送到 mailbox
		r.BcastAppend()

	default:
		log.Printf("unkown MsgType")
		return errors.New("unkown MsgType")
	}
	return nil
}

func StepCandidate(r *Raft, m pb.Message) error {
	log.Printf("StepCandidate begin ...")

	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Printf("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVoteResponse:

		//step01  不允自己重复投票给自己,因为已经投递过一次了
		if r.Vote == m.From {
			log.Println("r.Vote == m.To ", m.To)
			return nil
		}
		//step02 判断是否自己peers
		if _, ok := r.Prs[m.From]; !ok {
			log.Println("m.From is err ", m.From)
			return nil
		}

		//step03 //Reject 会被置为 True ，反之则会置为 False
		if m.Reject {
			r.votes[m.From] = false
		} else {
			r.votes[m.From] = true
		}
		//step04：如果有半数以上同意了则切换到 Leader 状态，半数以上节点拒绝了则切换到 Follower 状态

		agreeCount := 0
		noagreeCount := 0

		for k, v := range r.votes {
			if v {
				agreeCount = agreeCount + 1
			} else {
				noagreeCount = noagreeCount + 1
			}

			log.Println("MessageType_MsgRequestVoteResponse1  ", k, v, agreeCount, noagreeCount)
		}

		half := len(r.Prs)/2 + 1
		log.Println("MessageType_MsgRequestVoteResponse2  ", agreeCount, noagreeCount, half)

		if agreeCount >= half {
			r.becomeLeader()
		}

		if noagreeCount >= half {
			r.becomeFollower(r.Term, None)
		}

	default:
		log.Println("StepCandidate unkown msg type =", m.MsgType)
	}
	return nil
}

func StepFollower(r *Raft, m pb.Message) error {

	switch m.MsgType {

	case pb.MessageType_MsgPropose:
		// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries
		//Follower 不处理客户的请求

	case pb.MessageType_MsgAppend:
		// Follower：'MessageType_MsgAppend' contains log entries to replicate.
		r.electionElapsed = 0 // 收到 leader 的消息了，就不要想着再去竞争选举了
		r.Lead = m.From
		// 处理 leader 发过来的 oplog 消息
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0 //Follower收到leader心跳包 说明服务正常 不需要Leader election
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0 // 收到 leader 的消息了，就不要想着再去竞争选举了
		r.Lead = m.From
		r.handleSnapshot(m)
	default:
		log.Println(" StepFollower unkown msg type =", m.MsgType)
	}

	return nil
}

// 状态机步进，添加消息到 raftLog 中（未持久化）
//可变参数 https://www.youtube.com/watch?v=FiUYY0iW03M
//https://www.kancloud.cn/kancloud/the-way-to-go/72475
func (r *Raft) AppendEntry(es ...*pb.Entry) (accepted bool) {

	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}

	// use latest "last" index after truncate/append
	r.RaftLog.Append(es...)

	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	//Log replication逻辑
	//committed 是安全数据，不能被覆盖
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}
	/**
	// append 消息到 raftLog ，并且发送一条响应
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		r.send(pb.Message{To: m.From, MsgType: pb.MsgAppResp, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}**/
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	r.RaftLog.commitTo(m.Commit)
	//同步leader 提交点Commit
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse, Entries: m.Entries})
	//从接到rpc回应。
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *Raft) BcastAppend() {
	//log replication progress of each peers
	//Prs map[uint64]*Progress
	for id, value := range r.Prs {
		if id == r.id {
			continue
		}
		log.Println(" id=", id, "value =", value)
		r.sendAppend(id)
	}
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
func (r *Raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {

	//log replication progress of each peers
	//Prs map[uint64]*Progress
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}

	//step01-构造发送消息
	m := pb.Message{}
	m.To = to

	//获取消息内容
	term, _ := r.RaftLog.Term(pr.Next - 1)
	ents, _ := r.RaftLog.Entries(pr.Next, 100)

	m.Term = term

	// bcastAppend 的场景，就算是 entries 是空的，也会发送一条空的信息过去,
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	m.MsgType = pb.MessageType_MsgAppend

	// 投入到 mailbox 队列，等到 Ready 输出，发送网络
	r.send(m)
	return true
}

// send persists state to stable storage and then sends to its mailbox.
func (r *Raft) send(m pb.Message) {
	m.From = r.id

	// 添加信息到队列中，这个添加的信息，之后会作为 meesage 输出到 Ready 结构中；
	// 之后会发送过网络，m 是本地已经持久化了的消息
	// 经过 raft State Machine 处理过，确认需要发送网络；
	r.msgs = append(r.msgs, m)
}
