package main

import (
	"encoding/json"
	"io/ioutil"
	"log"

	"github.com/bacsorg/problem_single/go/bacs/problem/single"
	"github.com/bacsorg/problems/go/bacs/problem/decoder"
	"github.com/bunsanorg/broker/go/bunsan/broker"
	"github.com/bunsanorg/broker/go/bunsan/broker/service"
	"github.com/golang/protobuf/proto"
)

type ManualRequest struct {
	task *broker.Task
}

func (r *ManualRequest) WriteStatus(status broker.Status) error {
	log.Print("Recieved status")
	log.Print("Code: ", status.Code)
	log.Print("Reason: ", status.Reason)
	log.Print("Data: ", string(status.Data))
	return nil
}

func (r *ManualRequest) WriteResult(result broker.Result) error {
	log.Print("Recieved result")
	log.Print("Status: ", result.Status.String())
	log.Print("Reason: ", result.Reason)
	res, err := decoder.SingleResultDecoder.DecodeToText(result.Data)
	if err != nil {
		return err
	}
	log.Print("Data: ", res)
	log.Print("Log: ", string(result.Log))
	var single_result single.Result
	err = proto.Unmarshal(result.Data, &single_result)
	if err != nil {
		return nil
	}
	var timelimit uint64
	var memorylimit uint64
	timelimit = 0
	memorylimit = 0
	for _, testGroup := range single_result.TestGroup {
		for _, test := range testGroup.Test {
			usageT := test.Execution.ResourceUsage.TimeUsageMillis
			if usageT > timelimit {
				timelimit = usageT
			}
			usageM := test.Execution.ResourceUsage.MemoryUsageBytes
			if usageM > memorylimit {
				memorylimit = usageM
			}
		}
	}
	log.Print("Max time usage: ", timelimit)
	log.Print("Max memory usage: ", memorylimit)
	return nil
}

func (r *ManualRequest) WriteError(err error) error {
	log.Print("Error: ", err)
	return nil
}

func (r *ManualRequest) Task() *broker.Task {
	return r.task
}

func (r *ManualRequest) Ack() error {
	log.Printf("Acknowledging request")
	return nil
}

func (r *ManualRequest) Nack() error {
	log.Printf("Negatively acknowledging request")
	return nil
}

func NewManualRequest(taskfile string) (service.Request, error) {
	b, err := ioutil.ReadFile(taskfile)
	if err != nil {
		return nil, err
	}
	var task broker.Task
	err = json.Unmarshal(b, &task)
	if err != nil {
		return nil, err
	}
	return &ManualRequest{&task}, nil
}
