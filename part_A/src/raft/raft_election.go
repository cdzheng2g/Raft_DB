package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeMax - electionTimeMin)
	rf.electionTimeout = electionTimeMin + time.Duration(rand.Int63()%randRange)
}
func (rf *Raft) isElectionTimeoutLocked() bool {
	//return time.Since(rf.electionStart) > rf.electionTimeout
	electionStart := rf.electionStart
	return time.Now().Sub(electionStart) > rf.electionTimeout
}
func (rf *Raft) startElection(term int) { /*TODO：发送方的要票逻辑，除开自己之外都进行要票，给自己投上票，过半数才申请成为leader*/
	votes := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply) //生成空值对象 方便接收返回的信息
		if !ok {
			LOG(rf.me, rf.currentTerm, DError, "Send RequestVote Falied")
		}
		if rf.currentTerm < reply.Term {
			rf.becomeFollowerLocked(rf.currentTerm)
			return
		}
		if rf.contextLostLocked(Candidate, rf.currentTerm) {
			LOG(rf.me, rf.currentTerm, DError, "Context Lost(startElection)")
		}
		if reply.voteGranted {
			votes++
		}
		if votes > len(rf.peers)/2 {
			rf.becomeLeaderLocked()
			go rf.replicationTicker(term)
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate, term) {
		return
	}
	l := len(rf.log)
	for i := 0; i < len(rf.peers); i++ { //这里的i等于peer
		if i == rf.me {
			//投票给自己
			votes++
			rf.votedFor = rf.me
			continue
		}

		args := &RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogTerm:  rf.log[l-1].Term,
			LastLogIndex: l - 1,
		}
		go askVoteFromPeer(i, args)
		/* 异步进行*/

	}
	return
}
func (rf *Raft) electionTicker() { //设置过随机的超时时间、也设计了随机超市检查时间
	//如果处于选举超时时间则开始选举
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
		ms := 50 + (rand.Int63() % 300)
		//转换为Duration的 ns单位
		time.Sleep(time.Duration(ms) + time.Millisecond)
	}

}
func (rf *Raft) isMoreUpToDate(candidateIndex, candidateTerm int) bool { //选举日志的比较:取得当前日志长度，获取lastIndex、lastTerm
	l := len(rf.log)
	lastIndex, lastTerm := l-1, rf.log[l-1].Term
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log,Me:[%d]T%d,Candidate:[%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return lastIndex > candidateIndex
	}
	return lastIndex > candidateIndex
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject vote, higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		reply.voteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeLeaderLocked()
	}

	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Already voted S%d", args.CandidateId, rf.votedFor)
		reply.voteGranted = false
		return
	}

	if rf.isMoreUpToDate(args.CandidateId, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Reject voted,Candidate less up-to-date", args.CandidateId)
	}

	reply.voteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d", args.CandidateId)
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
