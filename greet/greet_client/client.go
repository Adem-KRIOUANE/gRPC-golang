package main

import (
	"context"
	"fmt"
	"gRPC-GO/greet/greetpb"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
)

func main() {
	fmt.Println("Hello I'm a client")

	cc, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("couldn't connect: %v", err)
	}

	defer cc.Close()

	c := greetpb.NewGreetServiceClient(cc)

	fmt.Printf("Created client:\n" /* %f", c*/)

	// doUnary(c)

	// doServerStreaming(c)

	// doClientStreaming(c)

	doBiDiStreaming(c)

}

func doUnary(c greetpb.GreetServiceClient) {
	fmt.Println("Starting to do a Unary RPC")
	req := &greetpb.GreetRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "ADEM",
			LastName:  "Kri",
		},
	}

	res, err := c.Greet(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Greet RPC: %v", err)
	}
	log.Printf("Response from Greet: %v", res.Result)
}

func doServerStreaming(c greetpb.GreetServiceClient) {
	fmt.Println("Starting to do a Server streaming RPC...")

	req := &greetpb.GreetManyTimesRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "ADEM",
			LastName:  "KRI",
		},
	}
	resStream, err := c.GreetManyTimes(context.Background(), req)

	if err != nil {
		log.Fatalf("error while calling GreetManyTimes RPC: %v", err)
	}

	for {

		msg, err := resStream.Recv()
		if err == io.EOF {
			//we've reached the end of the stream
			break
		}
		if err != nil {
			log.Fatalf("error while reading the stream: %v", err)
		}
		log.Printf("Response from GreetManyTimes: %v", msg.GetResult())
	}

}

func doClientStreaming(c greetpb.GreetServiceClient) {
	fmt.Println("Starting to do a Client streaming RPC...")

	requests := []*greetpb.LongGreetRequest{{
		Greeting: &greetpb.Greeting{
			FirstName: "Adem",
		},
	}, {
		Greeting: &greetpb.Greeting{
			FirstName: "Ahmed",
		},
	}, {
		Greeting: &greetpb.Greeting{
			FirstName: "Oussama",
		},
	}, {
		Greeting: &greetpb.Greeting{
			FirstName: "Mohamed",
		},
	},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "Sami",
			},
		},
	}

	stream, err := c.LongGreet(context.Background())
	if err != nil {
		log.Fatalf("error while calling LongGreet: %v", err)
	}
	// we iterate over our slice and send each messageindividually
	for _, req := range requests {
		fmt.Printf("Sending req: %v\n", req)
		stream.Send(req)
		time.Sleep(1000 * time.Millisecond)
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error while receiving response from LongGreet: %v", err)
	}
	fmt.Printf("LongGreet Response: %v\n", res)
}

func doBiDiStreaming(c greetpb.GreetServiceClient) {
	fmt.Println("Starting to do a BiDi streaming RPC...")

	requests := []*greetpb.GreetEveryoneRequest{
		{
			Greeting: &greetpb.Greeting{
				FirstName: "Adem",
			}},
		{Greeting: &greetpb.Greeting{
			FirstName: "Ahmed",
		}},
		{Greeting: &greetpb.Greeting{
			FirstName: "Oussama",
		}},
		{Greeting: &greetpb.Greeting{
			FirstName: "Mohamed",
		}},
		{Greeting: &greetpb.Greeting{
			FirstName: "Sami",
		}},
	}

	//we create a stream by invoking the client
	stream, err := c.GreetEveryone(context.Background())
	if err != nil {
		log.Fatalf("error while calling GreetEveryone: %v", err)
		return
	}

	waitc := make(chan struct{})

	//we send a bunch of messages to the client(go routine)
	go func() {
		// function to send a bunch of messages
		for _, req := range requests {
			fmt.Printf("\nSending message: %v\n", req)
			stream.Send(req)
			time.Sleep(2000 * time.Millisecond)
		}
		stream.CloseSend()
	}()

	//we recieve a bunch of messages from the client (go routine)
	go func() {
		// function to receive a bunch of messages
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error while receiving: %v", err)
				break
			}
			fmt.Printf("\n---> Receiving: %v\n", res.GetResult())
		}
		close(waitc)

	}()

	//block until everything is done
	<-waitc
}
