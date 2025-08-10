# ingestor-service/Dockerfile
FROM golang:1.19-alpine

WORKDIR /app

# 预先复制 go.mod 和 go.sum 以利用 Docker 的层缓存
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

# 编译 Go 应用
RUN go build -o /ingestor-server ./main.go

# 暴露 gRPC 端口
EXPOSE 50051

# 容器启动时运行的命令
CMD [ "/ingestor-server" ]