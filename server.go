package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/maguec/GRPC-Valkey/proto/helloworld"
	"github.com/valkey-io/valkey-go"
)

const (
	port = ":50051"
)

// Define a struct that implements the generated gRPC service interface
type server struct {
	pb.UnimplementedGreeterServer
	valkeyClients []valkey.Client
}

func dialLogger(s1 string, dialer *net.Dialer, tlsconfig *tls.Config) (net.Conn, error) {
	now := time.Now()
	conn, err := net.Dial("tcp", s1)
	fmt.Println("Dialing", s1, "took", time.Since(now).Milliseconds(), "ms")
	return conn, err
}

// Implement the SayHello method
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
  c := rand.Intn(10)
  fmt.Println("Using client", c)
  client := s.valkeyClients[c]
	cmds := make(valkey.Commands, 0, 100)
	for i := 0; i < 10; i++ {
		mykey := fmt.Sprintf("key-%d", rand.Intn(100000))
		cmds = append(cmds, client.B().Set().Key(mykey).Value("val").Build())
	}
	for _, resp := range client.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			panic(err)
		}
	}
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func main() {
  clients := make([]valkey.Client, 10)
	for i := 0; i < 10; i++ {
		client, err := valkey.NewClient(
			valkey.ClientOption{
				InitAddress: []string{"127.0.0.1:30001"},
				DialFn:      dialLogger,
			},
		)
		if err != nil {
			panic(err)
		}
		defer client.Close()
    clients[i] = client
	}
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{valkeyClients: clients})

	// Register reflection service on gRPC server.
	reflection.Register(s)

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
