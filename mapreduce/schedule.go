package mapreduce

import (
	"fmt"
	"sync"
)

//
// $chedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerCh@n will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part #2, 2B).
	//


	var wg sync.WaitGroup // init a wait group to know when all tasks have been completed
	wg.Add(ntasks) // add ntasks number of tasks 

	for i:=0; i<ntasks; i++{
		// spawn a goroutine for the task
		go func (task int){
			defer wg.Done() // mark this specific task as done only when the function is over

			// infinite for loop for task retry until success
			for {
				// fmt.Printf("waiting on worker :(")
				worker := <-registerChan
				debug("Task %d got worker %s", task, worker)
				args := DoTaskArgs{
					JobName: jobName,
					File: "",
					Phase: phase,
					TaskNumber: task,
					NumOtherPhase: n_other,
				}

				// only access the mapFiles if we are in map phase, otherwise leave arg as ""
				if phase == mapPhase{
					args.File = mapFiles[task]
				}

				var reply struct{}
				success := call(worker, "Worker.DoTask", &args, &reply)
				debug("Task %d success: %v", task, success)
				// task completed, return worker, and break
				go func(){registerChan <- worker} () //return the worker async so it doesn't block on channel
				if success {
					break
				} 
			}
			
		} (i) //passes in i as the arg to the anonymous function
		

		
	}
	wg.Wait() // waits until all ntasks/goroutines have completed after launching them concurrently


	fmt.Printf("Schedule: %v done\n", phase)
}
