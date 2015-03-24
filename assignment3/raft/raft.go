package raft

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
)

type Lsn uint64      //Log sequence number, unique for all time.
type ErrRedirect int // Implements Error interface.

//var lock sync.Mutex //Lock for appending to log

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

type LogEntry interface {
	Lsn() Lsn
	Data() Command
	Committed() bool
}

/*
type Event struct {
	event string
}
*/

//A command from client
type Command struct {
	Cmd        string
	Key        string
	ExpiryTime int64
	Length     int64
	Version    int64
	Value      string
}

type LogItem struct {
	LSN       Lsn
	DATA      Command
	COMMITTED bool
	TERM      uint64
}

func (l LogItem) Lsn() Lsn {
	return l.LSN
}
func (l LogItem) Data() Command {
	return l.DATA
}
func (l LogItem) Committed() bool {
	return l.COMMITTED
}

type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data Command) (LogEntry, error)
}

// --------------------------------------
// Raft setup
type ServerConfig struct {
	Id         int    //Id of server. Must be unique
	Hostname   string //name or ip of host
	ClientPort int    //port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

var serverRaftMap map[int]*Raft //map 5 servers
var ClusterInfo ClusterConfig
var serverRaftMapLock sync.Mutex

// Raft implements the SharedLog interface.
type Raft struct {
	// .... fill
	Log         []LogItem
	LastLsn     Lsn
	ServerID    int
	ClientPort  int
	LogPort     int
	LeaderID    int
	State       string
	kvChan      chan LogEntry
	eventCh     chan interface{}
	Term        uint64
	CommitIndex uint64
	VotedFor    int
	Lock        sync.Mutex
	LastApplied uint64
	NextIndex   []Lsn
	MatchIndex  []Lsn
}

//Commit channel to kvStore
//var kvChan chan LogEntry

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {

	//Get config.json file from $GOPATH/src/assignment2/
	filePath := os.Getenv("GOPATH") + "/src/assignment3/config.json"
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.New("Couldn't open config file from :" + filePath)
	}

	err = json.Unmarshal(file, &ClusterInfo)
	if err != nil {
		return nil, errors.New("Wrong format of config file")
	}

	raft := Raft{}
	for _, server := range config.Servers {

		if server.Id == thisServerId { //Config for this server
			raft.ServerID = thisServerId
			//raft.LeaderID = 0 //First server is leader by default
			raft.ClientPort = server.ClientPort
			raft.LogPort = server.LogPort
			raft.LastLsn = 0
			raft.State = FOLLOWER
			raft.Term = 0
			raft.CommitIndex = 0
			raft.VotedFor = -1
			raft.LastApplied = 0
			break
		}
	}
	raft.Log = append(raft.Log, LogItem{})

	raft.NextIndex = make([]Lsn, 5, 5)
	raft.MatchIndex = make([]Lsn, 5, 5)
	raft.kvChan = commitCh //Store channel
	raft.eventCh = make(chan interface{}, 5)

	//Going to start life as a follower
	//Need to implement election and other stuff
	go raft.loop()

	if serverRaftMap == nil {
		serverRaftMap = make(map[int]*Raft)
	}

	serverRaftMap[thisServerId] = &raft

	/*
			if thisServerId == raft.LeaderID {
				//We are the leader, so start hearbeater
				go heartBeater()
			} else {
				//Just a follower, listen for RPC from leader
		1		go appendRPCListener(raft.LogPort)
			}
	*/
	log.Print("Raft init, Server id:" + strconv.Itoa(raft.ServerID))
	return &raft, nil
}

/*
func (r *Raft) Append(data Command) (LogEntry, error) {

	//Check if leader. If not, send redirect
	if r.ServerID != r.LeaderID {
		return LogItem{}, ErrRedirect(r.LeaderID)
	}

	logItem := LogItem{r.LastLsn + 1, data, false}

	lock.Lock()
	r.Log = append(r.Log, logItem)
	r.LastLsn++
	lock.Unlock()

	return &logItem, nil
}
*/
// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(int(e))
}
