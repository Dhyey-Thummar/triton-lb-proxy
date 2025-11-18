sudo apt-get update
sudo apt-get install -y protobuf-compiler

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

git clone https://github.com/triton-inference-server/common.git /tmp/triton-common

cp /tmp/triton-common/protobuf/grpc_service.proto proto/
cp /tmp/triton-common/protobuf/model_config.proto proto/

rm -rf /tmp/triton-common

cd proto

protoc --go_out=. --go-grpc_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_opt=paths=source_relative \
    grpc_service.proto model_config.proto

go get google.golang.org/grpc
go get google.golang.org/protobuf/reflect/protoreflect
go get google.golang.org/protobuf/runtime/protoimpl

cd ..

go mod tidy