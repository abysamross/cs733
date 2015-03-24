package raft

import (
	"errors"
)

type AppendRPCArgs struct {
	Term         uint64
	LeaderId     int
	PrevLogIndex Lsn
	PrevLogTerm  uint64
	Log          []LogItem
	LeaderCommit uint64
}

type AppendRPCResults struct {
	Term    uint64
	Success bool
}

func (r *Raft) Append(data Command) (LogEntry, error) {
	//Check if leader. If not, send redirect

	if r.ServerID != r.LeaderID {
		return LogItem{}, ErrRedirect(r.LeaderID)
	}
	/*
		  	logItem := LogItem{r.LastLsn + 1, data, false}
			lock.Lock()
			r.Log = append(r.Log, logItem)
			r.LastLsn++
			lock.Unlock()
			return &logItem, nil
	*/
	responseCh := make(chan LogEntry)
	r.eventCh <- ClientAppend{data, responseCh}
	logItem := <-responseCh

	if logItem.Lsn() == 0 {
		return LogItem{}, ErrRedirect(r.LeaderID)
	}

	return logItem, nil
}

func (raft *Raft) appendRPC(server ServerConfig, appRpcArgs AppendRPCArgs, replyAppRpc *AppendRPCResults) error {

	serverRaftMapLock.Lock()
	serverRaft, valid := serverRaftMap[server.Id]
	serverRaftMapLock.Unlock()

	if !valid {
		return errors.New("Here: No such server")
	}

	responseCh := make(chan AppendRPCResults, 5)
	serverRaft.eventCh <- AppendRPC{appRpcArgs, responseCh}
	*replyAppRpc = <-responseCh

	return nil
}

/*
func (ap *AppendEntries) AppendEntriesRPC(args AppendRPCArgs, result *AppendRPCResults) error {

	if len(args.Log) > 0 {
		log.Print("AppendRPC received")
	} else {
		log.Print("Heartbeat received")
	}

	//Append to our local log
	for _, log := range args.Log {
		command := log.DATA

		raft.Append(command)
	}

	result.Success = true
	return nil
}

func appendRPCListener(port int) {
	appendRPC := new(AppendEntries)
	rpc.Register(appendRPC)

	listener, err := net.Listen("tcp", ":"+strconv.FormatInt(int64(port), 10))
	if err != nil {
		log.Print("AppendRCP error : " + err.Error())
		return
	}

	for {
		if conn, err := listener.Accept(); err != nil {
			log.Print("Accept error : " + err.Error())
		} else {
			go rpc.ServeConn(conn)
		}
	}
}
*/
