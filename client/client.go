package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"net/http"
	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/pb"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

type KvStoreClient struct {
	Store map[string]string
}

type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

type ClientReqInput struct {
	arg      *pb.ClientReqArgs
	response chan pb.ClientReqRet
}


type ClientResponse struct {
	ret  *pb.ClientResponse
	err  error
}

func (r *Raft) ClientRequest(ctx context.Context, arg *pb.ClientReqArgs) (*pb.ClientReqRet, error) {
	c := make(chan pb.ClientReqArgs)
	r.AppendChan <- ClientReqInput{arg: arg, response: c}
	result := <-c
	return &result, nil
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
	client_req:=pb.ClientReqArgs({Term:int64(0), Request_type: request_type, Key:key_kv, Value: value_kv})

	ret, err := connection.NewClientRequest(context.Background(), &req)

	clientResponseChan <- ClientResponse{ret: ret, err: err}

}

func (r *Raft) (ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func main() {
	// Take endpoint as input


	kv_store := server.kvstore.KVStore{C: make(chan util.InputChannelType), Store: make(map[string]string)}
	kvc := KvStoreClient{Store: make(map[string]string)}
	clientResponseChan := make(chan ClientResponse)
	primary:="127.0.0.1:3001"
	connection, e=connectToPeer(primary)
	if e!=nil{
		log.Fatal("Failed to connect to primary- %v", e)
	}
	else{
		log.Printf("Connected to Primary!")
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		request_type:=r.FormValue("request_type")
		value_kv:=r.FormValue("value")
		key_kv:=r.FormValue("key")
		clientResponseChan:=make(chan ClientResponse)
		go SendToPrimary(kvc, connection, request_type, value_kv, key_kv, clientResponseChan)
		log.Printf("Request sent, now waiting for response!")
		result := <-clientResponseChan
		log.Printf("Success Received!")
		
	)

	})
}
