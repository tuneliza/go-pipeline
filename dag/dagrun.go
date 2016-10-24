package dag

import (
	"sync"
	"fmt"
)

type TaskRunState int 
const (
	TaskRunState_WAIT 		TaskRunState = 0	// default zero value
	TaskRunState_RUN 		TaskRunState = 1

	TaskRunState_SUCCESS 	TaskRunState = 2
	TaskRunState_FAIL		TaskRunState = 3
	TaskRunState_SKIP 		TaskRunState = 4 	// for branching capability
	TaskRunState_RESIGN		TaskRunState = 5	// when dependencies upstream have not been met

	TaskRunState_STOPPED	TaskRunState = 6	// force stopped mid-execution
	TaskRunState_ERROR		TaskRunState = 7	// something weird has happened with sate transitions
)

// Tasks are to be stored as nodes of a Dag
type TaskRun struct{
	task *Task			
	ins []<-chan TaskRunState
	outs []chan<- TaskRunState
	state TaskRunState
}

func (tr *TaskRun) run(ctx interface{}) error { // <-stop chan bool // can interrupt execution by sending to this channel

	// if this is a root node, run task
	if len(tr.ins) == 0 {
		return tr.executeAndSignal(ctx)
	}

	// else wait until all incoming chnnels have signaled
	var wg sync.WaitGroup
	wg.Add(len(tr.ins))
	sigs := make([]TaskRunState, len(tr.ins))	
	
	for i := range tr.ins {
		go func(ix int){
			defer wg.Done()
			sigs[ix] = <-tr.ins[ix]
		}(i)
	}
	wg.Wait()

	// sort through signals
	// determine what to do based on signals received
	sig_count := make(map[TaskRunState]int)
	for _, s := range sigs {
		_, ok := sig_count[s]
		if ok {
			sig_count[s] = sig_count[s] + 1
		} else {
			sig_count[s] = 1
		}
	}

	// if all say SKIP, then this task is skipped 
	if sig_count[TaskRunState_SKIP] == len(sigs) {
		tr.state = TaskRunState_SKIP
		for _, out := range tr.outs {
			go func(c chan<- TaskRunState){ c<- TaskRunState_SKIP }(out)
		}
		return nil
	}

	// else if at least one FAIL/RESIGN when not BestEffort, then propagate RESIGN down the chain. 
	// or all SKIP/FAIL/RESIGN when BestEffort
	if (!tr.task.BestEffort && (sig_count[TaskRunState_FAIL] + sig_count[TaskRunState_RESIGN] > 0)) ||
		(tr.task.BestEffort && (sig_count[TaskRunState_FAIL] + sig_count[TaskRunState_RESIGN] + sig_count[TaskRunState_SKIP] == len(sigs))){

		tr.state = TaskRunState_RESIGN
		for _, out := range tr.outs {
			go func(c chan<- TaskRunState){ c<- TaskRunState_RESIGN }(out)
		}
		return nil
	}

	// if at least one SUCCESS and the rest are SKIP when not BestEffort 
	// or at least one SUCCESS and the rest are SKIP/FAIL/RESIGN  when BestEffort - but not ERROR
	// execute task
	if (sig_count[TaskRunState_SUCCESS] > 0) && (
		!tr.task.BestEffort && (sig_count[TaskRunState_SUCCESS] + sig_count[TaskRunState_SKIP] == len(sigs)) || 
		tr.task.BestEffort && (sig_count[TaskRunState_SUCCESS] + sig_count[TaskRunState_FAIL] + sig_count[TaskRunState_RESIGN] + sig_count[TaskRunState_SKIP] == len(sigs))){
		
		return tr.executeAndSignal(ctx)
	}	

	// otherwise the state is ERROR and weird... should never get here
	tr.state = TaskRunState_ERROR
	for _, out := range tr.outs {
			go func(c chan<- TaskRunState){ c<- TaskRunState_RESIGN }(out)
	}
	return fmt.Errorf("Task %s reached ERROR state - undefined state sransition", tr.task.ID)
}

// this is separated out for reuse and readability purposes
func (tr *TaskRun) executeAndSignal(ctx interface{}) error {
	tr.state = TaskRunState_RUN
	nextID, ok, err := tr.task.execFunc(ctx)	
	
	if ok { 
		tr.state = TaskRunState_SUCCESS
	} else { 
		tr.state = TaskRunState_FAIL
	}

	// signal completion to outgoing channels
	// if nextID unspecified => signal result to all
	if nextID == "" {
		for _, out := range tr.outs {
			go func(c chan<- TaskRunState){ c<- tr.state }(out)
		}
	} else {
		for i, out := range tr.outs {
			if nextID == tr.task.next[i].ID { 
				go func(c chan<- TaskRunState){ c<- tr.state }(out) 
			} else { 
				go func(c chan<- TaskRunState){ c<- TaskRunState_SKIP }(out) 
			}
		}
	}
	
	if err != nil {
		 return fmt.Errorf("Error executing task %s : %v", tr.task.ID, err)
	}
	return nil
}

type DagRun struct{
	dag *Dag 

	// a run instance contains maps of at least all TaskRuns (nodes) and all channels (edges)
	nodes 	map[string]*TaskRun  // TaskRuns by taskID
	edges []chan TaskRunState
	
	params map[string]interface{} 
	// messages map[string]map[string]interface{}
}

// Make a DagRun instance
// the dag structure and the run itself are separate because there may be many run-instances of the same dag,
// so we don't want to have a mess of channels to deal with
func Run (d *Dag, taskParams map[string]interface{}) *DagRun{  // return channel?? to signal finish?

	dr := DagRun{dag: d, params: taskParams}

	// build dagrun graph
	// first pass to create all nodes
	dr.nodes = make(map[string]*TaskRun)
	for i := range d.Tasks {
		dr.nodes[d.Tasks[i].ID] = &TaskRun{task: d.Tasks[i], 
			ins: make([]<-chan TaskRunState, len(d.Tasks[i].prior)),
			outs: make([]chan<- TaskRunState, len(d.Tasks[i].next)),
			state: TaskRunState_WAIT,
		} 
	}

	// make a sink channel at the end of the pipeline
	end := make(chan TaskRunState)
	endCount := 0

	// second pass (over TaskRuns) to set edges (signal channels)
	for id, tr := range dr.nodes {
		if len(tr.task.next) == 0 {
			tr.outs = append(tr.outs, end)
			endCount++
		}

		for j, pt := range tr.task.prior {
			ch := make (chan TaskRunState)  // channel between run of pt and this task's run
			tr.ins[j] = ch
			for k, target := range pt.next{
				if (target.ID == id) {
					dr.nodes[pt.ID].outs[k] = ch
				}
			}
		}	
	}

	// run all tasks in separate goroutines
	for id, n := range dr.nodes {
		go n.run(taskParams[id])
	}

	// wait for all tsks to finish
	for ; endCount > 0; endCount-- {
		<-end
	}

	return &dr
}

// NOP
func (dr *DagRun) ForceStop(){

}


// Print state of all tasks in the DagRun
func (dr *DagRun) PrintStatus(){  // might need locks for that if @ runtime
	for id, tr := range dr.nodes {
		fmt.Printf("task %s : state %v\n", id, tr.state)
	}
}
