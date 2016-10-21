// Package Dag provides primitives for the workflow construction: a Dag (Directed Acyclic Graph) of Tasks.
// Edges of the graph are directed and unweighted.
//
// The Dag and Task relationships are defined by the user. 

package dag

import (
	"fmt"
)

// A Dag is a collection of Tasks
// A Dag is built implicitly by creating Tasks and setting edges (dependencies) between them
type Dag struct {
	name string
	Tasks []*Task  // TODO: why not make this a map?
}

// Expected behavior
// If string is non-null, then branch --> TODO: make branching task explicitly definable?
type ExecFunc func(ctx interface{}) (taskId string, success bool, err error)


// A Task cannot exist without a Dag, can only belong to one Dag, and have dependency relations with Tasks of the same Dag
type Task struct {
	owner 		*Dag
	ID 			string		// unique name 
	execFunc 	ExecFunc	// user specified fxn (defines the task)
	next 		[]*Task  	// edges to the following Tasks
	prior 		[]*Task  	// edges to preceding Tasks
}

// create an empty Dag
func New(dname string) (*Dag){
	return &Dag{name: dname}
} 

// Add a Task to the Dag; error if a Task with that ID already exists in the Dag
func (d *Dag) MakeTask(id string, op ExecFunc) (*Task, error){
	if id == "" {
		return nil, fmt.Errorf("A Task with empty string for id is not allowed")
	}
	if d.hasTask(id) {
		return nil, fmt.Errorf("A Task with id %s already exists in this Dag", id)
	}

	n := Task{ID: id, owner: d, execFunc: op}
	d.Tasks = append(d.Tasks, &n)
	return &n, nil
}

func (d *Dag) hasTask(id string) bool {
	for _, n := range d.Tasks {
		if n.ID == id {
			return true
		}
	}
	return false
}  

// Set an edge to Task n from Task up [up -> n]. They must belong to the same Dag (error if not).
// Note: both Tasks must be created before setting an edge
func (n *Task) SetUpstream(up *Task) error{
	if !(n.owner.hasTask(up.ID)){
		return fmt.Errorf("Tasks %s and %s do not belong to the same Dag", up.ID, n.ID)
	}

	// check that it's not a duplicate edge

	up.next = append(up.next, n)
	n.prior = append(n.prior, up)
	
	return nil
}

// Set an edge from Task n to Task down [n -> down]. They must belong to the same Dag (error if not).
// Note: both Tasks must be created before setting an edge
func (n *Task) SetDownstream(down *Task) error{
		if !(n.owner.hasTask(down.ID)){
		return fmt.Errorf("Tasks %s and %s do not belong to the same Dag", down.ID, n.ID)
	}

	// check that it's not a duplicate edge

	n.next = append(n.next, down)
	down.prior = append(down.prior, n)

	return nil
}

// Print contents of the Dag to std out
func (d *Dag) Print() {
	for _, n := range d.Tasks {
		fmt.Printf("%s -> ", n.ID)
		for _, down := range n.next {
			fmt.Printf("%s ,", down.ID)
		}
		fmt.Print("\n")
	}
}
