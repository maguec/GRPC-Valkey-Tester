package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/maguec/GRPC-Valkey/proto/helloworld"
	"github.com/valkey-io/valkey-go"
)

const (
	port = ":50051"
)

func generateUncompressibleString(kb int) (string, error) {
	if kb < 0 {
		return "", fmt.Errorf("kilobytes cannot be negative")
	}

	if kb == 0 {
		return "", nil
	}

	targetBytes := kb * 1024

	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?`~ "
	charsetLen := byte(len(charset)) // Convert length to byte for modulo operation.
	randomBytes := make([]byte, targetBytes)
	_, err := io.ReadFull(rand.Reader, randomBytes)
	if err != nil {
		return "", fmt.Errorf("failed to read random bytes: %w", err)
	}
	resultChars := make([]byte, targetBytes)

	for i := 0; i < targetBytes; i++ {
		resultChars[i] = charset[randomBytes[i]%charsetLen]
	}

	return string(resultChars), nil
}

// Define a struct that implements the generated gRPC service interface
type server struct {
	pb.UnimplementedGreeterServer
	valkeyClient valkey.Client
}

func retrieveTokenFunc(yo valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
	username := "default"
	password := os.Getenv("TOKEN")
	return valkey.AuthCredentials{Username: username, Password: password}, nil
}

func valkeyDialLogger(ctx context.Context, s1 string, dialer *net.Dialer, tlsconfig *tls.Config) (net.Conn, error) {
	var conn net.Conn
	var err error
	now := time.Now()
	if tlsconfig != nil {
		tlsDialer := tls.Dialer{NetDialer: dialer, Config: tlsconfig}
		conn, err = tlsDialer.DialContext(ctx, "tcp", s1)
	} else {
		conn, err = net.Dial("tcp", s1)
	}
	log.Printf("Dialing %s took %v", s1, time.Since(now))
	return conn, err
}

// Implement the SayHello method
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	var err error
	cmds := make(valkey.Commands, 0, 100)
	for i := 0; i < 100; i++ {
		cmds = append(cmds, s.valkeyClient.B().Get().Key(fmt.Sprintf("key-%d", mrand.Intn(100000))).Build().ToPipe())
	}

	for _, resp := range s.valkeyClient.DoMulti(ctx, cmds...) {
		if err = resp.Error(); err != nil {
			return nil, err
		}
	}

	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func main() {
	host := os.Getenv("MEMORYSTORE_IP")
	if host == "" {
		panic("Need to set the MEMORYSTORE_IP env var")
	}
	caCertPool := x509.NewCertPool()
	certPEM, err := os.ReadFile("/tmp/ca.crt")
	if err != nil {
		fmt.Printf("Error reading certificate file: %v\n", err)
		return
	}
	caCertPool.AppendCertsFromPEM(certPEM)
	client, err := valkey.NewClient(
		valkey.ClientOption{
			InitAddress:           []string{fmt.Sprintf("%s:6379", host)},
			PipelineMultiplex:     8,
			DialCtxFn:             valkeyDialLogger,
			DisableAutoPipelining: true,
			AuthCredentialsFn:     retrieveTokenFunc,
			//SendToReplica: func(cmd valkey.Command) bool {
			//	return cfg.readReplica && cmd.IsReadOnly() && rand.Float64() < 0.5
			//},
			TLSConfig: &tls.Config{
				RootCAs: caCertPool,
				//	ClientSessionCache: tls.NewLRUClientSessionCache(1024),
			},
		},
	)
	defer client.Close()
	if err != nil {
		panic(err)
	}

  ctx := context.Background()

	exists, _ := client.Do(ctx, client.B().Exists().Key("key-99999").Build()).ToInt64()

	if exists == 1 {
		log.Printf("Test keys are already loaded")
	} else {

		log.Printf("Populating keyspace - starting")
		for i := 0; i < 100000; i++ {
      v, _ := generateUncompressibleString(10)
			client.Do(ctx, client.B().Set().Key(fmt.Sprintf("key-%d", i)).Value(v).Build().ToPipe())
		}
		log.Printf("Populating keyspace - complete")
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{valkeyClient: client})

	// Register reflection service on gRPC server.
	reflection.Register(s)

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
