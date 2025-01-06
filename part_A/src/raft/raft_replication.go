package raft

import (
	"time"
)

type LogEntry struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// 用一个term 和 日志号 定位一个日志
	PreLogIndex int
	PreLogTerm  int
	Entries     []LogEntry
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) replicationTicker(term int) { //NOTE:领导人发送心跳逻辑（心跳间隔是小于选举超时时间下界的）
	if !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			return
		}
		time.Sleep(replicateInterval)
	}
}

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		if !reply.Success {
			idx, term := args.PreLogIndex, args.PreLogTerm
			if idx != 0 && term == rf.log[idx].Term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
		}
		rf.matchIndex[peer] = args.PreLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			//更新匹配点
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}
		args := &AppendEntriesArgs{
			Term:     term,
			LeaderId: rf.me,
		}
		//NOTE：进行异步的RPC处理 发送心跳
		go replicateToPeer(peer, args)
	}
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	if args.PreLogIndex > len(rf.log) {
		//日志丢失太多
		return
	}

	rf.log = append(rf.log[:args.PreLogIndex+1], args.Entries...) //args.Entries是个切片 使用...，表示可变参数
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, accept log", args.LeaderId)
	//TODO:处理 领导提交
	// NOTE:接收到领导人心跳之后，重置自身选举时间
	rf.resetElectionTimerLocked()
}
