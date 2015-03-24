package raft

import (
	"log"
	"math/rand"
	"time"
)

const (
	timeout_follower = 500 * time.Millisecond
)

type ClientAppend struct {
	command    Command
	responseCh chan LogEntry
}

type AppendRPC struct {
	appRpcArgs AppendRPCArgs
	responseCh chan AppendRPCResults
}

type VoteRequest struct {
	voteReqArgs VoteRequestArgs
	responseCh  chan VoteRequestResult
}

type Timeout struct {
}

func (raft *Raft) loop() {

	for {
		switch raft.State {
		case FOLLOWER:
			//log.Println("ServerID: ", raft.ServerID, "State: ", FOLLOWER)
			raft.follower()

		case CANDIDATE:
			//log.Println("ServerID: ", raft.ServerID, "State: ", CANDIDATE)
			raft.candidate()

		case LEADER:
			//log.Println("ServerID: ", raft.ServerID, "State: ", LEADER)
			raft.leader()

		default:
			raft.dispLogInfo("Unknown State")
			break
		}
	}
}

func (raft *Raft) follower() {

	//log.Println("Inside func follower()", "ServerID: ", raft.ServerID)

	timer := time.AfterFunc(timeout_follower+(time.Duration(rand.Intn(100))*time.Millisecond), func() { raft.eventCh <- Timeout{} })

	for {
		event := <-raft.eventCh

		switch event.(type) {

		case ClientAppend:
			raft.dispLogInfo("Append from client received")
			//log.Println("Append from client received")
			ev := event.(ClientAppend)
			logItem := LogItem{0, ev.command, false, raft.Term}
			ev.responseCh <- logItem

		case AppendRPC:
			ev := event.(AppendRPC)

			if len(ev.appRpcArgs.Log) == 0 {
				//heartbeat
				//log.Println("Hearbeat received")
				raft.dispLogInfo("Heartbeat Received")

			} else {
				//appendrpc
				//log.Println("AppendRPC received")
				raft.dispLogInfo("AppendRPC Received")
			}

			//Server sending append has lower term
			if raft.Term > ev.appRpcArgs.Term {

				replyAppRpc := AppendRPCResults{raft.Term, false}
				ev.responseCh <- replyAppRpc

			} else if raft.Log[ev.appRpcArgs.PrevLogIndex].TERM != ev.appRpcArgs.PrevLogTerm {

				if raft.Term < ev.appRpcArgs.Term {
					raft.Term = ev.appRpcArgs.Term
					raft.VotedFor = -1
				}

				//What abut making follower log consistent with leader log?

				raft.LeaderID = ev.appRpcArgs.LeaderId

				//Remove everything from here and after it

				raft.Log = raft.Log[:ev.appRpcArgs.PrevLogIndex]
				replyAppRpc := AppendRPCResults{raft.Term, false}
				ev.responseCh <- replyAppRpc

			} else {
				//Update term

				if raft.Term < ev.appRpcArgs.Term {
					raft.Term = ev.appRpcArgs.Term
					raft.VotedFor = -1
				}

				raft.LeaderID = ev.appRpcArgs.LeaderId

				raft.Lock.Lock()
				raft.Log = append(raft.Log, ev.appRpcArgs.Log...)
				raft.Lock.Unlock()

				//update commit index: min(leader commit, last entry added)
				if ev.appRpcArgs.LeaderCommit > raft.CommitIndex {

					lastIndex := raft.Log[len(raft.Log)-1].Lsn()

					min := uint64(lastIndex)
					if min > ev.appRpcArgs.LeaderCommit {
						min = ev.appRpcArgs.LeaderCommit
					}

					raft.CommitIndex = min
				}

				if raft.CommitIndex > raft.LastApplied {
					//Apply to state machine
					for i, _ := range raft.Log[raft.LastApplied+1 : raft.CommitIndex+1] {

						raft.Log[i].COMMITTED = true
						raft.kvChan <- raft.Log[i]
					}

					raft.LastApplied = raft.CommitIndex
				}

				replyAppRpc := AppendRPCResults{raft.Term, true}
				ev.responseCh <- replyAppRpc

			}

			timer.Reset(timeout_follower + (time.Duration(rand.Intn(100)) * time.Millisecond))

		case VoteRequest:

			ev := event.(VoteRequest)

			termCandidate := ev.voteReqArgs.Term
			voteStatus := false

			//What about denying this voteRequest if t'was received within minimum follower timeout
			//of hearing from a current leader?
			//NOTE: This voteRequest is mostly sent by an isolated or removed server.

			if raft.Term > termCandidate {

				voteStatus = false
				//log.Println("Vote request denied")
				raft.dispLogInfo("Vote request denied")

			} else if (raft.VotedFor == -1) || (raft.VotedFor == int(ev.voteReqArgs.CandidateID)) {

				if ev.voteReqArgs.LastLogTerm > raft.Log[raft.LastLsn].TERM {

					raft.Term = termCandidate
					raft.VotedFor = int(ev.voteReqArgs.CandidateID)
					voteStatus = true
					//log.Println("Vote Cast")
					raft.dispLogInfo("Vote Cast")

				} else if (ev.voteReqArgs.LastLogTerm == raft.Log[raft.LastLsn].TERM) && (ev.voteReqArgs.LastLogIndex >= raft.LastLsn) {

					raft.Term = termCandidate
					raft.VotedFor = int(ev.voteReqArgs.CandidateID)
					voteStatus = true
					//log.Println("Vote Cast")
					raft.dispLogInfo("Vote Cast")

				} else {

					voteStatus = false
					//log.Println("Vote request denied")
					raft.dispLogInfo("Vote request denied")

				}

			} else {

				voteStatus = false
				//log.Println("Vote request denied")
				raft.dispLogInfo("Vote request denied")

			}

			ev.responseCh <- VoteRequestResult{raft.Term, voteStatus}

			timer.Reset(timeout_follower + (time.Duration(rand.Intn(100)) * time.Millisecond))

		case Timeout:
			//log.Println("Timeout")
			raft.dispLogInfo("Timeout received")
			raft.State = CANDIDATE
			return
		}
	}
}

func (raft *Raft) candidate() {

	//log.Println("Inside func candidate()", "ServerID: ", raft.ServerID)
	raft.Term++
	raft.VotedFor = -1
	raft.dispLogInfo("")

	tot_votes := 0
	for _, server := range ClusterInfo.Servers {

		if raft.ServerID == server.Id {
			tot_votes++
			raft.VotedFor = raft.ServerID
			continue
		}

		lastLogTerm := raft.Log[raft.LastLsn].TERM
		voteReqArgs := VoteRequestArgs{raft.Term, uint64(raft.ServerID), raft.LastLsn, lastLogTerm}
		voteReqReply := VoteRequestResult{}

		err := raft.requestVote(server, voteReqArgs, &voteReqReply)

		if err != nil {
			log.Println(err.Error())
			continue
		}

		if voteReqReply.VoteReceived == true {
			tot_votes++
		}

		if tot_votes > len(ClusterInfo.Servers)/2 {
			raft.dispLogInfo("Becoming Leader")
			raft.State = LEADER
			raft.LeaderID = raft.ServerID
			return
		}
	}

	timer := time.AfterFunc(timeout_follower+(time.Duration(rand.Intn(100))*time.Millisecond), func() { raft.eventCh <- Timeout{} })

	for {

		event := <-raft.eventCh

		switch event.(type) {

		//Candidate gets ClientAppend
		//Resend the same event channel after timeout as there is no leader now.
		case ClientAppend:
			//log.Println("Append from client received")
			raft.dispLogInfo("Append from client received")
			time.AfterFunc(timeout_follower, func() { raft.eventCh <- event })

			//Candidate gets AppendRPC
		case AppendRPC:
			//log.Println("AppendRPC received")
			raft.dispLogInfo("AppendRPC received")
			ev := event.(AppendRPC)
			//success := false

			//Check if leader
			if ev.appRpcArgs.Term > raft.Term {
				raft.Term = ev.appRpcArgs.Term
				raft.VotedFor = -1
				raft.State = FOLLOWER

				//Resend event to be handled after becoming a follower
				raft.eventCh <- event
				timer.Stop()

				return

			} else {
				replyAppRpc := AppendRPCResults{raft.Term, false}
				ev.responseCh <- replyAppRpc
			}

			/*
				replyAppRpc := AppendRPCResults{raft.Term, success}
				ev.responseCh <- replyAppRpc

				if success == true {
					timer.Stop()
					return
				}
			*/

		case VoteRequest:
			ev := event.(VoteRequest)

			//Candidate doesn't vote for others
			voteReqReply := VoteRequestResult{raft.Term, false}
			//log.Println("Vote request denied")
			raft.dispLogInfo("Vote request denied")
			ev.responseCh <- voteReqReply

		case Timeout:
			raft.dispLogInfo("Timeout Received")
			raft.Term++
			raft.VotedFor = -1
			return
		}

	}

}

func (raft *Raft) leader() {

	//log.Println("Inside func leader()", "ServerID: ", raft.ServerID)
	raft.dispLogInfo("I am Leader")
	timer := time.AfterFunc(0, func() { raft.eventCh <- Timeout{} })

	for i, _ := range raft.NextIndex {
		raft.NextIndex[i] = raft.LastLsn + 1
		raft.MatchIndex[i] = 0
	}

	for {
		event := <-raft.eventCh

		switch event.(type) {

		case ClientAppend:
			raft.dispLogInfo("Append from client received")
			ev := event.(ClientAppend)
			logItem := LogItem{raft.LastLsn + 1, ev.command, false, raft.Term}
			raft.Log = append(raft.Log, logItem)
			raft.LastLsn++

			ev.responseCh <- logItem

		case AppendRPC:
			raft.dispLogInfo("AppendRPC received")
			ev := event.(AppendRPC)

			//Check if received from actual leader
			if ev.appRpcArgs.Term > raft.Term {
				raft.State = FOLLOWER
				raft.Term = ev.appRpcArgs.Term
				raft.VotedFor = -1

				ev.responseCh <- AppendRPCResults{raft.Term, true}
				return
			} else {
				//remain leader
				ev.responseCh <- AppendRPCResults{raft.Term, false}
			}

		case VoteRequest:
			ev := event.(VoteRequest)

			//Check if received from actual leader
			if ev.voteReqArgs.Term > raft.Term {
				if ev.voteReqArgs.LastLogTerm > raft.Log[raft.LastLsn].TERM {

					raft.State = FOLLOWER
					raft.Term = ev.voteReqArgs.Term
					raft.VotedFor = int(ev.voteReqArgs.CandidateID)
					ev.responseCh <- VoteRequestResult{raft.Term, true}
					raft.dispLogInfo("Vote Cast")
					timer.Stop()
					return

				} else if (ev.voteReqArgs.LastLogTerm == raft.Log[raft.LastLsn].TERM) && (ev.voteReqArgs.LastLogIndex >= raft.LastLsn) {

					raft.State = FOLLOWER
					raft.Term = ev.voteReqArgs.Term
					raft.VotedFor = int(ev.voteReqArgs.CandidateID)
					ev.responseCh <- VoteRequestResult{raft.Term, true}
					raft.dispLogInfo("Vote Cast")
					timer.Stop()
					return

				} else {
					//remain leader
					ev.responseCh <- VoteRequestResult{raft.Term, false}
					raft.dispLogInfo("Vote request denied")

				}

			} else {
				//remain leader
				ev.responseCh <- VoteRequestResult{raft.Term, false}
				raft.dispLogInfo("Vote request denied")
			}

		case Timeout:
			raft.dispLogInfo("Heartbeat timeout")
			raft.heartBeater()
			timer.Reset(150 * time.Millisecond)

			if raft.State == LEADER {
				continue
			} else {
				timer.Stop()
				return
			}
		}
	}

}

func (raft *Raft) dispLogInfo(dispMsg string) {
	if raft.State == LEADER {
		log.Println("---------------------------------")
	}

	log.Print("ServerID: ", raft.ServerID, " [", " S: ", raft.State, " | ", " T: ", raft.Term, " | ", " L: ", raft.LeaderID, " | ", " V: ", raft.VotedFor, " ] ", " : ", dispMsg)
}
