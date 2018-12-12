package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"time"
	"encoding/json"
	"strings"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/starter-code-lab2/pb"
)

// Struct off of which we shall hang the Raft service


type Raft struct {
	NewClientRequestChan chan ClientReqInput
	PrePrepareMsgChan chan PrePrepareInput
	PrepareMsgChan chan PrepareInput
	CommitMsgChan chan CommitInput
}

type KvStoreServer struct {
	Store map[string]string
}

type ClientReqInput struct {
	Arg      *pb.ClientReqArgs
	Response chan pb.ClientReqRet
}


type ClientResponse struct {
	ret  *pb.ClientReqRet
	err  error
	node string
}
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func tamper(digest string) string {
	return RandStringRunes(len(digest))
}

type MsgRet struct {
	ret  *pb.MsgRet
	err  error
	peer string
}

type PrePrepareInput struct {
	Arg      *pb.PrePrepareArgs
	Response chan pb.MsgRet
}

type PrepareInput struct {
	Arg      *pb.PrepareArgs
	Response chan pb.MsgRet
}

type CommitInput struct {
	Arg      *pb.CommitArgs
	Response chan pb.MsgRet
}

type logEntry struct {
	sequencenumber int64             
	clientReq  *pb.ClientReqArgs 
	prePrepare    *pb.PrePrepareArgs 
	pre        []*pb.PrepareArgs  
	com        []*pb.CommitArgs  
	prepared       bool 
	committed      bool
	committedLocal bool
}

func (r *Raft) NewClientRequest(ctx context.Context, arg *pb.ClientReqArgs) (*pb.ClientReqRet, error) {
	c := make(chan pb.ClientReqRet)
	 r.NewClientRequestChan <- ClientReqInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}
func (r *Raft) Commit(ctx context.Context, arg *pb.CommitArgs) (*pb.MsgRet, error) {
	c := make(chan pb.MsgRet)
	r.CommitMsgChan <- CommitInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) PrePrepare(ctx context.Context, arg *pb.PrePrepareArgs) (*pb.MsgRet, error) {
	c := make(chan pb.MsgRet)
	r.PrePrepareMsgChan <- PrePrepareInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}
func (r *Raft) Prepare(ctx context.Context, arg *pb.PrepareArgs) (*pb.MsgRet, error) {
	c := make(chan pb.MsgRet)
	r.PrepareMsgChan <- PrepareInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func Digest(object interface{}) string {
	msg, err := json.Marshal(object)
	if err != nil {
		// return "", err
		log.Fatal("Cannot make digest")
	}
	return Hash(msg)
}

func Hash(content []byte) string {
	h := sha256.New()
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 4000
	const DurationMin = 1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r))
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}


func checkifprepared(entry logEntry) bool {
	ppm := entry.prePrepare
	if ppm != nil {
		countvalid := 0
		//log.Printf("-%v-", entry.pre)
		for i := 0; i < len(entry.pre); i++ {
			ppm_e := entry.pre[i]
			if ppm_e.Sequenceno==ppm.Sequenceno && ppm_e.Digest==ppm.Digest{
				countvalid+=1
			}
		}
		return countvalid>=2
	}else{
		return false
	}
}

func checkifcommitlocal(entry logEntry) bool {
	isprep:=checkifprepared(entry)
	if isprep{
		countvalid:=0
		for i := 0; i < len(entry.com); i++ {
			ppm_e := entry.com[i]
			if ppm_e.Sequenceno==entry.prePrepare.Sequenceno && ppm_e.Digest==entry.prePrepare.Digest{
				countvalid+=1
			}
		}
		return countvalid>=2
	}else{
		return false
	}
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int, client string, kvs *KvStoreServer, byzantine bool) {
	raft := Raft{NewClientRequestChan: make(chan ClientReqInput), PrePrepareMsgChan: make(chan PrePrepareInput), PrepareMsgChan: make(chan PrepareInput), CommitMsgChan: make(chan CommitInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)
	clientConn, _ := connectToPeer(client)
	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}
	pbftmessageChan:=make(chan MsgRet)
	clientResponseChan:=make(chan ClientResponse)
	type AppendResponse struct {
		ret  *pb.AppendEntriesRet
		err  error
		peer string
	}

	type VoteResponse struct {
		ret  *pb.RequestVoteRet
		err  error
		peer string
	}
	sequenceno:=int64(-1)
	maxlen:=int64(0)
	var logentries []logEntry
	// Run forever handling inputs from various channels
	for {
		select {
		case op := <-s.C:
			// We received an operation from a client
			// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			// TODO: Use Raft to make sure it is safe to actually run the command.
			s.HandleCommand(op)
		case clr := <-raft.NewClientRequestChan:
			log.Printf("Recd New client request.")
			request:=clr.Arg
			timestamp:=request.Timestamp
			digest:=Digest(request)
			sequenceno+=1
			preprepare:=pb.PrePrepareArgs{Sequenceno: sequenceno, Digest: digest, Node: id, Clientrequest: request}
			for p, c := range peerClients {
				go func(c pb.RaftClient, p string) {
					time.Sleep(10 * time.Millisecond)
					ret, err := c.PrePrepare(context.Background(), &preprepare)
					pbftmessageChan<-MsgRet{ret: ret, err:err, peer: p}
				}(c, p)
			}

			log_entry:= logEntry{sequencenumber: sequenceno, clientReq: request, prePrepare: &preprepare, pre: make([]*pb.PrepareArgs, maxlen), com: make([]*pb.CommitArgs, maxlen), prepared: false, committed: false, committedLocal: false}
			logentries = append(logentries, log_entry)
			log.Printf("Appended to log entry")
			response:=pb.ClientReqRet{Didwork:1, Timestamp: timestamp}
			clr.Response<-response
		case clr := <-raft.PrePrepareMsgChan:
			log.Printf("Recd pre-prepare request..")
			//check if prepare message valid!
			verified:=true
			digest := Digest(clr.Arg.Clientrequest)
			if digest != clr.Arg.Digest{
				verified=false
			}
			if verified{
				if(byzantine){
					digest=tamper(digest)
				}	
				preparemsg:=pb.PrepareArgs{Sequenceno: clr.Arg.Sequenceno, Digest: digest, Node: id}
				log.Printf("Appending new entry to logs")
				log_entry:= logEntry{sequencenumber: clr.Arg.Sequenceno, clientReq: clr.Arg.Clientrequest, prePrepare: clr.Arg, pre: make([]*pb.PrepareArgs, maxlen), com: make([]*pb.CommitArgs, maxlen), prepared: false, committed: false, committedLocal: false}
				old_prepares:=log_entry.pre
				old_prepares=append(old_prepares, &preparemsg)
				log_entry.pre=old_prepares
				logentries = append(logentries, log_entry)
				for p, c := range peerClients {
					go func(c pb.RaftClient, p string) {
						time.Sleep(10 * time.Millisecond)
						ret, err := c.Prepare(context.Background(), &preparemsg)
						pbftmessageChan<-MsgRet{ret: ret, err:err, peer: p}
					}(c, p)
				}
			}
			resp:= pb.MsgRet{Sequenceno: sequenceno, Success: verified, Typeofmsg: "pre_prepare", Node: id}
			clr.Response <- resp
		case clr := <-raft.PrepareMsgChan:
			log.Printf("Recd prepare request..")
			//check if prepare message valid!
			prep_msg:=clr.Arg
			prepared:=false
			valid:=true
			digest := logentries[prep_msg.Sequenceno].prePrepare.Digest
			if digest != prep_msg.Digest {
				valid=false
			}
			if valid{
				log.Printf("Seqno: %v", prep_msg.Sequenceno )
				old_entry := logentries[prep_msg.Sequenceno]
				prev_prepares := old_entry.pre
				prev_prepares = append(prev_prepares, prep_msg)
				old_entry.pre = prev_prepares
				logentries[prep_msg.Sequenceno] = old_entry
				//TODO: fix next 3 lines tomorrow morning.
				prepared=checkifprepared(old_entry)
				old_entry.prepared = prepared
				logentries[prep_msg.Sequenceno] = old_entry
				if prepared{
					log.Printf("PREPARED!!")
					commitmsg:=pb.CommitArgs{Sequenceno: prep_msg.Sequenceno, Digest: prep_msg.Digest, Node: id}
					for p, c := range peerClients {
						go func(c pb.RaftClient, p string) {
							time.Sleep(10 * time.Millisecond)
							ret, err := c.Commit(context.Background(), &commitmsg)
							pbftmessageChan<-MsgRet{ret: ret, err:err, peer:p}
						}(c, p)
					}
					preventry:=logentries[prep_msg.Sequenceno]
					prevcommits:=preventry.com
					prevcommits=append(prevcommits,&commitmsg)
					preventry.com=prevcommits
					logentries[prep_msg.Sequenceno]=preventry
				}

			}
			resp:= pb.MsgRet{Sequenceno: sequenceno, Success: valid, Typeofmsg: "prepare", Node: id}
			clr.Response <- resp
		case clr := <-raft.CommitMsgChan:
			com_msg:=clr.Arg
			valid:=true
			log.Printf("Recd commit request..")
			digest := logentries[com_msg.Sequenceno].prePrepare.Digest
			if digest != com_msg.Digest {
				valid=false
			}
			if valid{
				preventry:=logentries[com_msg.Sequenceno]
				prevcommits:=preventry.com
				prevcommits=append(prevcommits,com_msg)
				preventry.com=prevcommits
				logentries[com_msg.Sequenceno]=preventry
				iscommitedlocal:=checkifcommitlocal(preventry)
				if iscommitedlocal{
					log.Printf("COMMIT LOCAL")
					cr:=preventry.clientReq
					totalop := strings.Split(cr.Operation, ":")
					oper := totalop[0]
					key := totalop[1]
					val := totalop[2]
					res := pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
					if oper == "SET" {
						kvs.Store[key] = val
						res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
					} else if oper == "GET" {
						val = kvs.Store[key]
						res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
					}
					cr.Res=&res
					cr.Client=id
					go func(c pb.RaftClient) {
						ret, err := c.NewClientRequest(context.Background(), cr)
						clientResponseChan <- ClientResponse{ret: ret, err: err, node: id}
					}(clientConn)
				}

			
			}
			resp:= pb.MsgRet{Sequenceno: sequenceno, Success: valid, Typeofmsg: "commit", Node: id}
			clr.Response <- resp

		}
	}
	log.Printf("Strange to arrive here")
}
