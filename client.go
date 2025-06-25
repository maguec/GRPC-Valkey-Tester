package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jamiealquiza/tachymeter"
	"github.com/maguec/metermaid"
	"golang.org/x/exp/rand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/alexflint/go-arg"

	pb "github.com/maguec/GRPC-Valkey/proto/helloworld"
)

var args struct {
	Runs      int `help:"number of run throughs" default:"100" arg:"env:RUN_COUNT"`
	Workers      int `help:"number of threads" default:"100" arg:"env:RUN_COUNT"`
}

func worker(tach *tachymeter.Tachymeter, mm *metermaid.Metermaid, runs int) {
	// Set up a connection to the server.
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials())) // Note: Use TLS in production
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	name := "world"
	for i := 0; i < runs; i++ {
		// Contact the server and print out its response.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		now := time.Now()
		_, err := c.SayHello(ctx, &pb.HelloRequest{Name: name})
		tach.AddTime(time.Since(now))
		mm.Add()
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		//log.Printf("Greeting: %s", r.GetMessage())
	}
}

func main() {
	wg := sync.WaitGroup{}
  arg.MustParse(&args)
	tach := tachymeter.New(&tachymeter.Config{Size: args.Runs*args.Workers})
	mm := metermaid.New(&metermaid.Config{Size: args.Runs*args.Workers})
	for i := 0; i < args.Workers; i++ {
		wg.Add(1)
		go func() {
			worker(tach, mm, args.Runs)
			wg.Done()
		}()
	}
	wg.Wait()
	results := tach.Calc()
	fmt.Println("------------------ Latency ------------------")
	fmt.Printf(
		"Max:\t\t%s\nMin:\t\t%s\nP95:\t\t%s\nP99:\t\t%s\nP99.9:\t\t%s\n\n",
		results.Time.Max,
		results.Time.Min,
		results.Time.P95,
		results.Time.P99,
		results.Time.P999,
	)
	rates := mm.Calc()
	fmt.Println("-------------------- Rate -------------------")
	fmt.Printf(
		"MaxRate:\t%.1f/s\nMinRate:\t%.1f/s\nP95Rate:\t%.1f/s\nP99Rate:\t%.1f/s\nP99.9Rate:\t%.1f/s\n",
		rates.MaxRate, rates.MinRate, rates.P95Rate, rates.P99Rate, rates.P999Rate,
	)
}
