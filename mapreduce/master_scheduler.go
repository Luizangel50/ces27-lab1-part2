package mapreduce

import (
	"log"
	"sync"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {

	var (
		wg        sync.WaitGroup
		filePath  string
		worker    *RemoteWorker
		operation *Operation
		counter   int
	)

	// Allocating the memory of an empty chan with RETRY_OPERATION_BUFFER length
	master.failedOperationChan = make(chan *Operation, RETRY_OPERATION_BUFFER)

	log.Printf("Scheduling %v operations\n", proc)

	counter = 0
	for filePath = range filePathChan {
		operation = &Operation{proc, counter, filePath}
		counter++

		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runOperation(worker, operation, &wg)

	}

	wg.Wait()

	// Close the chan because cannot accept more push/pop on the chan
	close(master.failedOperationChan)

	// Iterate over chan elements, without waiting for push/pop because
	// the chan is already closed
	for failedOperation := range master.failedOperationChan {

		// Pick one idle worker
		worker = <-master.idleWorkerChan

		// Increment the counter of the WaitGroup
		wg.Add(1)

		// Run the failed operation by an idle worker
		go master.runOperation(worker, failedOperation, &wg)
	}	

	// Barrier that waits all wg.Done() from each new goroutine
	// created earlier
	wg.Wait()

	log.Printf("%vx %v operations completed\n", counter, proc)
	return counter
}

// runOperation start a single operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup) {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		err  error
		args *RunArgs
	)

	log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)

	args = &RunArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))

	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
		wg.Done()		
		master.failedWorkerChan <- remoteWorker
		master.failedOperationChan <- operation
	} else {
		wg.Done()	
		master.idleWorkerChan <- remoteWorker
	}
}
