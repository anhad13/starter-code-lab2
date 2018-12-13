package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"net"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"net/http"
	"github.com/starter-code-lab2/pb"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

type KvStoreClient struct {
	Store map[string]string
}

type Raft struct {
	NewClientRequestChan chan ClientReqInput
	PrePrepareMsgChan chan PrePrepareInput
	PrepareMsgChan chan PrepareInput
	CommitMsgChan chan CommitInput
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

func RunServer(r *Raft, port int) {
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
	log.Printf("Trying to connect to peer.")
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

func SendToPrimary(kv_c *KvStoreClient, connection pb.RaftClient, request_type string, value_kv string, key_kv string, clientResponseChan chan ClientResponse){
	log.Printf("sending request to primary: "+request_type+"_"+ key_kv+"_"+value_kv)
	client_req:=pb.ClientReqArgs{Operation: request_type+":"+ key_kv+":"+value_kv, Timestamp: time.Now().UnixNano()}

	ret, err := connection.NewClientRequest(context.Background(), &client_req)

	clientResponseChan <- ClientResponse{ret: ret, err: err}

}


func main() {
	// Take endpoint as input
	//var port int
	var pbftPort int
	flag.IntVar(&pbftPort, "pbft", 3005, "Port on which client should listen to PBFT responses")
	server := grpc.NewServer()
	pbft := Raft{NewClientRequestChan: make(chan ClientReqInput), PrePrepareMsgChan: make(chan PrePrepareInput), PrepareMsgChan: make(chan PrepareInput), CommitMsgChan: make(chan CommitInput)}
	
	go RunServer(&pbft, pbftPort)

	kv_store := KVStore{C: make(chan InputChannelType), store: make(map[string]string)}
	kvc := KvStoreClient{Store: make(map[string]string)}
	//clientResponseChan := make(chan ClientResponse)
	primary:="127.0.0.1:3001"	
	connection, e:=connectToPeer(primary)
	if e!=nil{
		log.Fatal("Failed to connect to primary- %v", e)
	}else{
		log.Printf("Connected to Primary!")
	}
	pb.RegisterKvStoreServer(server, &kv_store)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		request_type:=r.FormValue("request_type")
		value_kv:=r.FormValue("value")
		key_kv:=r.FormValue("key")
		clientResponseChan:=make(chan ClientResponse)
		go SendToPrimary(&kvc, connection, request_type, value_kv, key_kv, clientResponseChan)
		log.Printf("Request sent, now waiting for response!")
		result := <-clientResponseChan
		validResp:=0
		log.Printf("Initial succ recieved - %v", result)
		for validResp<2{
			cr:=<-pbft.NewClientRequestChan
			if cr.Arg.Timestamp==result.ret.Timestamp{
				log.Printf("Recd. Matched timestamp!")
				validResp+=1
				log.Printf("Answer: %v", cr.Arg.Res.Result)
			}else{
				//log.Printf("Did not match!")
			}
		}
		log.Printf("Success Received!")
	})
	fmt.Println(http.ListenAndServe(":8000", nil))
}
