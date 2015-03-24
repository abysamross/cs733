package raft

import (
	"errors"
)

type VoteRequestArgs struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex Lsn
	LastLogTerm  uint64
}

type VoteRequestResult struct {
	Term         uint64
	VoteReceived bool
}

func (raft *Raft) requestVote(server ServerConfig, voteReqArgs VoteRequestArgs, reply *VoteRequestResult) error {

	serverRaftMapLock.Lock()
	serverRaft, valid := serverRaftMap[server.Id]
	serverRaftMapLock.Unlock()

	if !valid {
		return errors.New("Here: No such server")
	}

	responseCh := make(chan VoteRequestResult, 5)
	serverRaft.eventCh <- VoteRequest{voteReqArgs, responseCh}
	*reply = <-responseCh

	return nil
}
