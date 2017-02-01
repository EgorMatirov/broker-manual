package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"

	"github.com/bunsanorg/broker/go/bunsan/broker/worker"
	"github.com/bunsanorg/pm/go/bunsan/pm"
)

var repositoryConfig = flag.String("repository-config", "",
	"bunsan::pm configuration file")
var tmpdir = flag.String("tmpdir", "", "Temporary directory for workers")
var taskfile = flag.String("taskfile", "", "File with task info")

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
	request, err := NewManualRequest(*taskfile)
	if err != nil {
		return err
	}
	err = worker.Do(request)
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
