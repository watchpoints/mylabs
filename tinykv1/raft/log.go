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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// unstable contains all unstable entries and snapshot.
	// they will be saved into storage.
	//unstable unstable

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	return newLogWithSize(storage)
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	//??????i?????????
	dummyIndex, err := l.storage.FirstIndex()
	if err == nil {
		log.Println("storage.FirstIndex failed")
		return 0, nil
	}
	dummyIndex = dummyIndex - 1
	if i < dummyIndex || i > l.LastIndex() {
		return 0, nil
	}

	//??????????????????
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)

	return 0, nil
}

// newLogWithSize returns a log using the given storage and max
// message size.
func newLogWithSize(storage Storage) *RaftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	//lastIndex, err := storage.LastIndex()
	//if err != nil {
	//	panic(err) // TODO(bdarnell)
	//}
	//log.unstable.offset = lastIndex + 1
	//log.unstable.logger = logger
	// Initialize our committed and applied pointers to the time of the last compaction.
	// ??????????????????????????????committted???applied ??????????????????
	//??????????????? apply ??????????????????????????????????????????????????????????????? commit/apply ????????????
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *RaftLog) Append(ents ...*pb.Entry) uint64 {

	//01??????????????????
	if len(l.entries) == 0 {
		return l.LastIndex()
	}

	after := ents[0].Index - 1 //why -1
	//02 ?????? Index?????? Index >committed
	if after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	//l.unstable.truncateAndAppend(ents)

	for i := range ents {
		l.entries = append(l.entries, *ents[i])
	}
	return l.LastIndex()
}

// apply id
func (l *RaftLog) AppliedTo(i uint64) {
	if i == 0 {
		return
	}
	//
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	// apply index ????????? raftLog ???
	l.applied = i
}

//
func (l *RaftLog) Entries(i, maxsize uint64) ([]pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}
	return l.slice(i, l.LastIndex()+1, maxsize)
}

func (l *RaftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {

	return nil, nil
}

// commit ??????, ?????? tocommit ??????
// ????????????????????????????????? commit
// 1. heartbeat 	: ?????? leader ?????????????????????
// 2. maybeAppend 	: ?????? leader append oplog ???????????????
// 3. maybeCommit	: leader ?????? commit ??????
func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	} else {
		log.Println("the leader tocommit is less")
	}
}
