// ingestor-service/main.go
package main

import (
	"context"
	"fmt"
	pb "smartfleet/ingestor/proto" // 导入生成的包
	"time"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"log"
	"net"
)

const (
	port            = ":50051"
	rabbitMQURL     = "amqp://guest:guest@message-queue:5672/"
	telemetryQueue = "telemetry_queue"
)

// server 结构体实现了我们在 proto 文件中定义的服务接口
type server struct {
	pb.UnimplementedTelemetryServiceServer
	rabbitMQChannel *amqp.Channel
}

// StreamTelemetryData 是核心实现
func (s *server) StreamTelemetryData(stream pb.TelemetryService_StreamTelemetryDataServer) error {
	log.Println("Client connected to stream...")
	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			log.Println("Client disconnected.")
			return ctx.Err()
		default:
			// 从流中接收一个数据包
			packet, err := stream.Recv()
			if err == io.EOF {
				log.Println("Stream ended by client.")
				return nil
			}
			if err != nil {
				log.Printf("Error receiving packet: %v", err)
				return err
			}

			log.Printf("Received packet from vehicle %s", packet.VehicleId)

			// 将 Protobuf 消息转换为 JSON 字符串以便推送到 RabbitMQ
			jsonData, err := protojson.Marshal(packet)
			if err != nil {
				log.Printf("Failed to marshal packet to JSON: %v", err)
				continue // 继续处理下一个包
			}

			// 将数据推送到 RabbitMQ
			err = s.rabbitMQChannel.Publish(
				"",             // exchange
				telemetryQueue, // routing key (queue name)
				false,          // mandatory
				false,          // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        jsonData,
				},
			)
			if err != nil {
				log.Printf("Failed to publish message to RabbitMQ: %v", err)
				// 理论上这里应该有重试逻辑
			}

			// 发送回执 (Ack) 给客户端
			if err := stream.Send(&pb.Ack{PacketId: packet.PacketId, Received: true}); err != nil {
				log.Printf("Failed to send ack: %v", err)
			}
		}
	}
}

func main() {
	// 1. 连接 RabbitMQ
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// 声明一个队列，如果不存在则创建
	_, err = ch.QueueDeclare(
		telemetryQueue, // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	// 2. 启动 gRPC 服务器
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterTelemetryServiceServer(s, &server{rabbitMQChannel: ch})

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}