#!/bin/bash

distributedQueryId=""
localQueryId=""
operatorId=""
value=""

usage() {
    echo "Usage: $0 --distributed-query-id ID --local-query-id ID --operator-id ID --value VALUE"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --distributed-query-id)
            distributedQueryId="$2"
            shift 2
            ;;
        --local-query-id)
            localQueryId="$2"
            shift 2
            ;;
        --operator-id)
            operatorId="$2"
            shift 2
            ;;
        --value)
            value="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if [[ -z "$distributedQueryId" ]]; then
    echo "Error: --distributed-query-id is required"
    usage
fi
if [[ -z "$localQueryId" ]]; then
    echo "Error: --local-query-id is required"
    usage
fi
if [[ -z "$operatorId" ]]; then
    echo "Error: --operator-id is required"
    usage
fi
if [[ -z "$value" ]]; then
    echo "Error: --value is required"
    usage
fi

grpcurl -import-path ./grpc/ -plaintext -proto grpc/SingleNodeWorkerRPCService.proto -d '{"distributedQueryId": "'"$distributedQueryId"'", "localQueryId": "'"$localQueryId"'", "operatorId": '"$operatorId"', "value": '"$value"'}' localhost:8080 WorkerRPCService/MockStatistics
