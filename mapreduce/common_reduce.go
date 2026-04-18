package mapreduce

import (
	"encoding/json"
	"os"
	"io"
	"sort"
)


func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce m@nages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk!
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used J$ON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the g0lang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// J$ON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part #I).
	//





	
	// create a map with word -> slice of all values
	counts := make(map[string][]string)
	// iterate from 0 to nMap
	for m:=0; m < nMap; m++{
		fileName := reduceName(jobName, m, reduceTask)
		readFile, err := os.Open(fileName)
		if err != nil {
			debug("Error opening file: %v\n", err)
		}
		
		//must decode the file from JSON
		dec := json.NewDecoder(readFile)
		
		// iterate decode the JSON and create KeyValue structs for each pair, append it to the slice with the unique key
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF{
				break
			}
			if err != nil {
				debug("Error reading file: %v\n", err)
				continue
			}
			counts[kv.Key] = append(counts[kv.Key], kv.Value)
		}
		readFile.Close()
	}
	
	// slice of only keys to sort
	keys := []string{}
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys) //sort in asc. order

	// create output file and encoder to write to it
	writeFile, err := os.Create(outFile)
	if err != nil {
		debug("Error creating file: %v\n", err)
	}
	enc := json.NewEncoder(writeFile)


	//call reduce on sorted strings
	for _, k := range keys {
		//writes directly to the output file after calling reduce
		err := enc.Encode(KeyValue{k, reduceF(k, counts[k])})
		if err != nil {
			debug("Error writing to file: %v\n", err)
		}
	}
	writeFile.Close()
}
