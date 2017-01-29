package main

import (
    "encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bunsanorg/broker/go/bunsan/broker"
	"github.com/bunsanorg/broker/go/bunsan/broker/service"
	"github.com/bunsanorg/broker/go/bunsan/broker/worker"
	"github.com/bacsorg/problems/go/bacs/problem/decoder"
	"github.com/bunsanorg/pm/go/bunsan/pm"
)

type testRequest struct {
	task         *broker.Task
}

func (r *testRequest) WriteStatus(status broker.Status) error {
    log.Print("STATUS:")
	log.Print("Code:")
	log.Print(status.Code)
	log.Print("Reason:")
	log.Print(status.Reason)
	log.Print("Data:")
	log.Print(string(status.Data))
    return nil
}

func (r *testRequest) WriteResult(result broker.Result) error {
    log.Print("RESULT:")
	log.Print("Status:")
	log.Print(result.Status.String())
	log.Print("Reason: ")
	log.Print(result.Reason)
	log.Print("Data:")
	res, err := decoder.SingleResultDecoder.DecodeToText(result.Data)
	if err != nil {
		return err
	}
	log.Print(res)
	log.Print("Log:")
    log.Print(string(result.Log))
    return nil
}

func (r *testRequest) WriteError(err error) error {
    log.Print("ERROR:")
    log.Print(err)
    return nil
}

func (r *testRequest) Task() *broker.Task {
	return r.task
}

func (r *testRequest) Ack() error {
	log.Printf("Acknowledging request")
    return nil
}

func (r *testRequest) Nack() error {
	log.Printf("Negatively acknowledging request")
    return nil
}


func NewTestRequest(task *broker.Task) service.Request {
	return &testRequest{task}
}

var repositoryConfig = flag.String("repository-config", "",
	"bunsan::pm configuration file")
var tmpdir = flag.String("tmpdir", "", "Temporary directory for workers")
var taskfile = flag.String("taskfile", "", "File with task info")

func abortOnSignal(
	reader service.RequestReader,
	workerPool worker.WorkerPool) {

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received signal %q, aborting", sig)
		reader.Close()
		workerPool.Abort()
	}()
}

// scope
func run() error {

	repository, err := pm.NewRepository(*repositoryConfig)
	if err != nil {
		return err
	}
	defer repository.Close()

    dir, err := ioutil.TempDir(*tmpdir, "")
    if err != nil {
        return err
    }
    defer os.RemoveAll(dir)
    worker := worker.NewWorker(repository, dir)

	log.Print("Processing tasks...")
    b, err := ioutil.ReadFile(*taskfile)
    if err != nil {
        return err
    }
    var task broker.Task
    err = json.Unmarshal(b, &task)
    err = worker.Do(NewTestRequest(&task))
	if err != nil {
		return err
	}
    return nil
}

func main() {
	flag.Parse()
	if *repositoryConfig == "" {
		log.Fatal("Must set -repository-config")
	}
    
	if *taskfile == "" {
		log.Fatal("Must set -taskfile")
	}

	err := run()
	if err != nil {
		log.Fatal(err)
	}
}
