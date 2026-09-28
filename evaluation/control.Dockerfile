FROM ubuntu:latest


RUN apt update
RUN apt install -y golang-go ca-certificates build-essential iputils-ping
RUN go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

ENV PATH="$PATH:~/go/bin/"

ENTRYPOINT ["sleep", "infinity"]
