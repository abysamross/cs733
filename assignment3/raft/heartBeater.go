package raft

import (
	"log"
)

func (raft *Raft) heartBeater() {

	//votes := 0 //Votes from majority
	//startLogIndex := raft.getLatestLogIndex()

	//Send appendRPC to all server
	//starting from startLogIndex
	for _, server := range ClusterInfo.Servers {

		if raft.ServerID == server.Id {
			// The current running server
			continue
		}

		/*
			client, err := rpc.Dial("tcp", ":"+strconv.Itoa(server.LogPort))
				if err != nil {
					// log.Print("AppendRPC Dial error on port:" + strconv.Itoa(server.LogPort))
					log.Print("Server ", server.Id, " down")
					continue
				}
		*/

		//Create args to call RPC
		var logSlice []LogItem

		if raft.LastLsn >= raft.NextIndex[server.Id] {

			nextIndex := raft.NextIndex[server.Id]
			logSlice = raft.Log[nextIndex:]
		}

		prevLogIndex := raft.NextIndex[server.Id] - 1
		prevLogTerm := raft.Log[prevLogIndex].TERM

		/*
			if startLogIndex > raft.CommitIndex {
				//Add if more entires are added
				logSlice = raft.Log[startLogIndex : startLogIndex+1] //Only one entry for now
				//logSlice = raft.Log[startLogIndex:startLogIndex] //Only one entry for now
			}
		*/
		args := AppendRPCArgs{raft.Term, raft.LeaderID, prevLogIndex, prevLogTerm, logSlice, uint64(raft.CommitIndex)} //Send slice with new entires

		var reply AppendRPCResults

		/*
			err = client.Call("AppendEntries.AppendEntriesRPC", args, &reply)
		*/
		err := raft.appendRPC(server, args, &reply)

		if err != nil {
			//log.Print("RPC fail :" + err.Error())
			log.Print(err.Error())
			continue
		}

		if reply.Term > raft.Term {
			raft.State = FOLLOWER
			raft.Term = reply.Term
			raft.VotedFor = -1
			break
		}
		//Count votes
		if reply.Success == true {
			//votes++
			raft.NextIndex[server.Id] = raft.LastLsn + 1
			raft.MatchIndex[server.Id] = raft.LastLsn + 1
		} else {
			raft.NextIndex[server.Id]--
		}
	}

	for i := raft.CommitIndex + 1; i <= uint64(raft.LastLsn); i++ {
		votes := 0

		for j := 0; j < 5; j++ {
			if uint64(raft.MatchIndex[j]) >= i && raft.Log[i].TERM == raft.Term {
				votes++
			}
		}
		if votes >= len(ClusterInfo.Servers)/2 { //Majority
			// log.Print("Got majority")
			//Send to commit channel, KV Store will be waiting
			raft.kvChan <- raft.Log[i]

			//Update status as commited
			raft.Lock.Lock()
			raft.Log[i].COMMITTED = true
			raft.Lock.Unlock()

			raft.CommitIndex = i
		}
	}
	//Sleep for some time before sending next heart beat
	// time.Sleep(200 * time.Millisecond)
	//time.Sleep(2 * time.Second) //for debuging
}

func (raft *Raft) getLatestLogIndex() int64 {

	for i, item := range raft.Log {
		if !item.Committed() {
			return int64(i) //First uncommitted entry
		}
	}

	return -1
}
