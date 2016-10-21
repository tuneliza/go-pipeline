package main

import (
	"github.com/TuneLab/go-pipeline/dag"
	"fmt"
)

// contains a description of tasks and their dependencies

// this is test code for now

func main(){

// build example dag

/*
	d := dag.New("v_example")
	one, _:= d.MakeTask("job_1", printMyID)
	two, _:= d.MakeTask("job_2", printMyID)
	three, _:= d.MakeTask("sink", printMyID)
	three.SetUpstream(one)
	three.SetUpstream(two) */
	
/*	d := dag.New("linear_example")
	one, _:= d.MakeTask("source", printMyID)
	two, _:= d.MakeTask("job_1", printMyID)
	err := one.SetDownstream(two)
	fmt.Printf("%v \n",err)*/

/*	d := dag.New("parallel_example")
	d.MakeTask("job_1", printMyID)
	d.MakeTask("job_2", printMyID) */

/*	d := dag.New("fanout_example")
	one, _:= d.MakeTask("source", printMyID)
	two, _:= d.MakeTask("job_1", printMyID)
	three, _:= d.MakeTask("job_2", printMyID)
	four, _ := d.MakeTask("sink", printMyID)
	three.SetUpstream(one)
	two.SetUpstream(one)
	two.SetDownstream(four) */

	d := dag.New("branch_example")
	one, _:= d.MakeTask("source", printMyIDBranchToSink)
	two, _:= d.MakeTask("job_1", printMyID)
	three, _:= d.MakeTask("job_2", printMyID)
	four, _ := d.MakeTask("sink", printMyID)
	three.SetUpstream(two)
	two.SetUpstream(one)
	four.SetUpstream(one) 

	d.Print()

	param := make(map[string]interface{})
	param["source"] = "source"
	param["job_1"] = "job_1"
	param["job_2"] = "job_2"
	param["sink"] = "sink"

	dr := dag.Run(d, param)
	dr.PrintStatus()

}

//example function
func printMyID(ctx interface{}) (taskId string, success bool, err error){
	n := ctx.(string)
	fmt.Printf("My id is %s\n", n)
	return "", true, nil
}

func printMyIDBranchToSink(ctx interface{}) (taskId string, success bool, err error){
	n := ctx.(string)
	fmt.Printf("My id is %s\n", n)
	return "sink", true, nil
}