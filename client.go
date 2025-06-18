package main

import (
	"context"
	"log"
	"sync"
	"time"

	"golang.org/x/exp/rand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/maguec/GRPC-Valkey/proto/helloworld"
)

func worker() {
	// Set up a connection to the server.
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials())) // Note: Use TLS in production
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	name := "world"
	for i := 0; i < 100; i++ {
		// Contact the server and print out its response.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		_, err := c.SayHello(ctx, &pb.HelloRequest{Name: name})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		//log.Printf("Greeting: %s", r.GetMessage())
	}
}

func main() {
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			worker()
			wg.Done()
		}()
	}
	wg.Wait()

}
