package guestbook

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

/* function to set up handlers */
func init() {
	http.HandleFunc("/json", jsonHandler)
}

/* function to handle json requests */
func jsonHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r)
	c := appengine.NewContext(r)
	log.Infof(c, "request")

	// Read body from request into variable b
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Infof(c, "reading body")
		http.Error(w, err.Error(), 500)
		return
	}

	/* interface{} is essentially object in Java */
	var f interface{}
	// store the json object mapping into f
	err = json.Unmarshal(b, &f)
	//log.Infof(c, "body:"+string(b))
	if err != nil {
		log.Infof(c, "marshalling: "+err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	/*{
		headers (optional) {...},
		body: {
			entityName: string,
			keys: [key1,key2,...,key],
			values: [
				[val1, val2, val3...],
				[val1, val2, val3...]
			]
		}
	}*/

	// unpack json into a map
	m := asMap(f)
	// headers := m["headers"].(map[string]interface{})
	body := asMap(m["body"])
	entityName := asString(body["entityName"])
	log.Infof(c, entityName)
	keys := asArray(body["keys"])
	vals := asArray(body["values"])

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	count := 0
	var datastoreKeys []*datastore.Key
	var datastoreProps []datastore.PropertyList

	// loop through each entry in payload
	for _, val := range vals {
		var props datastore.PropertyList
		vSet := asArray(val)
		count = count + 1
		// loop through each key-value pair in each entry
		for i, v := range vSet {
			k := asString(keys[i])
			//log.Infof(c, k)
			switch v.(type) {
			case string:
				//log.Infof(c, asString(v))
				props = append(props, datastore.Property{Name: k, Value: asString(v)})
			case float64:
				//log.Infof(c, strconv.FormatFloat(asFloat(v), 'f', 5, 64))
				props = append(props, datastore.Property{Name: k, Value: asFloat(v)})
			case int:
				//log.Infof(c, strconv.FormatInt(v.(int64), 10))
				props = append(props, datastore.Property{Name: k, Value: asInt(v)})
			case bool:
				//log.Infof(c, strconv.FormatBool(v.(bool)))
				props = append(props, datastore.Property{Name: k, Value: v.(bool)})
			default:
				fmt.Println(k, "is of a type I don't know how to handle")
			}
		}
		key := datastore.NewIncompleteKey(c, entityName, nil)
		/* ----SINGLE PUT-------*/
		/*
			_, errrr := datastore.Put(c, key, &props)
			if errrr != nil {
				log.Infof(c, "ERRRRR!"+errrr.Error())
			}
		*/
		datastoreKeys = append(datastoreKeys, key)
		datastoreProps = append(datastoreProps, props)

		if count%300 == 0 {
			log.Infof(c, strconv.Itoa(count))
			log.Infof(c, strconv.Itoa(len(datastoreKeys)))
			_, storeerror := datastore.PutMulti(c, datastoreKeys[count-300:count], datastoreProps[count-300:count])
			if storeerror != nil {
				log.Infof(c, storeerror.Error())
				http.Error(w, storeerror.Error(), 500)
			}

		}

	}

	output, err := json.Marshal(f)
	if err != nil {
		log.Infof(c, "marshalling json")
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Write(output)
}

/* cast functions to cast object to concrete types */
func asMap(o interface{}) map[string]interface{} {
	return o.(map[string]interface{})
}

func asArray(o interface{}) []interface{} {
	return o.([]interface{})
}

func asInt(o interface{}) int {
	return o.(int)
}

func asFloat(o interface{}) float64 {
	return o.(float64)
}

func asString(o interface{}) string {
	return o.(string)
}
