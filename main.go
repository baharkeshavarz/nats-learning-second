package main

import (
	proto2 "NATS-learning/proto"
	"fmt"
	"github.com/golang/protobuf/proto"
	stan "github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats"
	"runtime"
	"time"
)

// protoc -Iproto proto/user.proto --go_out=./proto ======>>> proto generator

var nc *nats.Conn
var users map[string]string

const (
	clusterID = "test-cluster"
	clientID  = "user-client"
	durableID = "user-client-durable"
)

func main() {
	var err error

	nc, err = nats.Connect(nats.DefaultURL)
	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL("http://localhost:4233"))

	if err != nil {
		fmt.Println(err)
	}

	myUser := proto2.User{Id: 2}
	data, err := proto.Marshal(&myUser)
	if err != nil || myUser.Id == 0 {
		fmt.Println(err)
		fmt.Println("Problem with parsing the user Id.")
		return
	}
	msg, err := nc.Request("GetUserById", data, 100*time.Millisecond)
	if err == nil && msg != nil {
		myUser := proto2.User{}
		err := proto.Unmarshal(msg.Data, &myUser)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println(myUser.String())
	}

	// Subscribe with manual ack mode, and set AckWait to 5 seconds
	aw, _ := time.ParseDuration("5s")
	sub, err := sc.QueueSubscribe("GetUsers", "GetUsers", GetStreamData,
		stan.DurableName(durableID),
		stan.MaxInflight(1),
		stan.SetManualAckMode(),
		stan.AckWait(aw))

	if err != nil {
		fmt.Println(err.Error())
	}
	// Set no limits for this subscription

	sub.SetPendingLimits(-1, -1)


	runtime.Goexit()

}

func GetStreamData(msg *stan.Msg) {
	{
		// if we don't send ack client NO1 is going to resend its message
		err := msg.Ack()
		if err != nil {
			println(err.Error())
		}
		myUser := proto2.User{}
		_ = proto.Unmarshal(msg.Data, &myUser)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("look :", myUser.String())
		return
	}
}
