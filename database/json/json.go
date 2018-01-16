package main

import (
	"encoding/json"
	//"encoding/json"
	"fmt"
)

type DataEntries struct {
	EntityName string
	Payloads   [][]SingleEntry
}

type SingleEntry struct {
	Key   string
	Value string
}

func ParsePutTask(data []byte) DataEntries {
	var dat map[string]interface{}
	if err := json.Unmarshal(data, &dat); err != nil {
		fmt.Printf(err.Error())
	}
	reqBody := dat["body"].(map[string]interface{})
	entityId := reqBody["entityName"].(string)
	payloads := reqBody["payload"].([]interface{})
	var entries [][]SingleEntry
	for _, entry := range payloads {
		parsed := entry.(map[string]interface{})
		var payload []SingleEntry
		for k, v := range parsed {
			pair := SingleEntry{Key: k, Value: v.(string)}
			payload = append(payload, pair)
		}
		entries = append(entries, payload)
	}
	task := DataEntries{EntityName: entityId, Payloads: entries}
	fmt.Println(task)
	return task
}

func main() {
	fmt.Printf("hello this is json package \n")
	byt := []byte(`{"body": {"entityName": "ent", "payload": [{"Key1": "val1","Key2": "val2","Key3": "val3"},{"Key1": "val4","Key2": "val5","Key3": "val6"}]}}`)
	// fmt.Println(ParsePutTask(byt))
	ParsePutTask(byt)
}
