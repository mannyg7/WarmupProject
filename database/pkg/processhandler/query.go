package processhandler

import (
	//"bufio"
	//"bytes"
	"context"
	//"encoding/csv"
	"encoding/json"
	"fmt"
	//"io"
	"io/ioutil"
	"net/http"
	//"sort"
	//"WarmupProject/database/datastorehandler"
	//"WarmupProject/database/filehandler"
	"WarmupProject/database/pkg/helper"
	"strconv"
	"strings"

	//"cloud.google.com/go/storage"
	"google.golang.org/appengine"
	//"google.golang.org/appengine/blobstore"
	"google.golang.org/appengine/datastore"
	//"google.golang.org/appengine/file"
	"google.golang.org/appengine/log"
)

/* function to handle query request from user */
func QueryHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	/* headers to allow CORS */
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("content-type", "application/json")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")
	w.Header().Set("content-type", "application/json")

	// Read body from request into variable b
	log.Debugf(c, "reading body")
	b, err := ioutil.ReadAll(r.Body)
	log.Debugf(c, "Finished reading:"+string(b))
	defer r.Body.Close()
	if err != nil {
		log.Errorf(c, "reading body error: "+err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	/* interface{} is essentially object in Java */
	var f interface{}
	// store the json object mapping into f
	err = json.Unmarshal(b, &f)
	log.Debugf(c, "finished unmarshalling")
	if err != nil {
		log.Errorf(c, "marshalling: "+err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	/*{
		columns: [columnName1, columnName2...],
		entity: entityName,
		filterCond: [],
		filterVal: [],
		order: orderRule,
		limit: limitNumber
	}
	*/
	// unpack json into a map
	var m map[string]interface{}
	m = helper.AsMap(f)
	var entityName string
	if entity, ok := m["entity"]; ok {
		entityName = helper.AsString(entity)
		log.Debugf(c, "entity name finished")
	} else {
		log.Errorf(c, "missing entityName: "+err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	q := datastore.NewQuery(entityName)
	/*
		if cols, ok := m["columns"]; ok {
			columns := asStringArray(cols)
			log.Debugf(c, "columns finished")
			q = q.Project(columns...)
		}
	*/

	if filterCond, ok := m["filterCond"]; ok {
		if filterVal, ok := m["filterVal"]; ok {
			filterConditions := helper.AsStringArray(filterCond)
			log.Debugf(c, "filter conditions finished")
			filterValues := helper.AsFloatArray(filterVal)
			log.Debugf(c, "filter values finished")
			if len(filterConditions) == len(filterValues) {
				for i, cond := range filterConditions {
					fieldName := strings.Split(cond, " ")[0]
					log.Debugf(c, fieldName)
					q = q.Order(fieldName)
					q = q.Filter(cond, filterValues[i])
					log.Debugf(c, cond)
					log.Debugf(c, strconv.FormatFloat(filterValues[i], 'f', 5, 64))
				}
			} else {
				log.Errorf(c, "filter condition and filter value length mismatch: "+err.Error())
			}
		}
	}

	if odr, ok := m["order"]; ok {
		order := helper.AsString(odr)
		log.Debugf(c, "order finished")
		q = q.Order(order)
	}

	if lmt, ok := m["limit"]; ok {
		limit := int(helper.AsFloat(lmt))
		log.Debugf(c, "limit finished")
		q = q.Limit(limit)
	}

	//var propLists []datastore.PropertyList
	iter := q.Run(c)
	//fmt.Fprintln(w, q)
	//fmt.Fprintln(w, iter)

	if err != nil {
		log.Errorf(c, "query error: "+err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	if cols, ok := m["columns"]; ok {
		columns := helper.AsStringArray(cols)
		log.Debugf(c, "columns finished")
		if n, ok := m["entityName"]; ok {
			res := saveJSONResponse(c, iter, columns, helper.AsString(n))
			w.Write(res)
		}
	} else {
		var defaultcols []string
		if n, ok := m["entityName"]; ok {
			res := saveJSONResponse(c, iter, defaultcols, helper.AsString(n))
			w.Write(res)
		}
	}
}

/* helper function to convert Property array to json object */
func saveJSONResponse(c context.Context, iter *datastore.Iterator, cols []string, entName string) []byte {

	var vals []map[string]interface{}
	//var res []byte
	//fmt.Fprintln(w, cols)

	for {
		var p datastore.PropertyList
		var propToWrite datastore.PropertyList
		//var props []datastore.Property
		_, err := iter.Next(&p)

		if err == datastore.Done {
			//log.Errorf(c, "datastore Done")
			break // No further entities match the query.
		}
		if err != nil {
			//log.Errorf(c, "fetching next Person: %v", err)
			break
		}
		//fmt.Fprintln(w, p)
		m := make(map[string]interface{})

		//HACK: PROJECTION
		for _, prop := range p {

			if len(cols) != 0 {

				if helper.In(prop.Name, cols) {
					propToWrite = append(propToWrite, prop)
					//fmt.Fprintln(w, i, cols[i])
					m[prop.Name] = prop.Value
					//fmt.Fprintln(w, prop.Value)
				}
			} else {
				m[prop.Name] = prop.Value
			}
		}
		// save processed data to datastore
		vals = append(vals, m)
		key := datastore.NewIncompleteKey(c, entName, nil)
		_, e := datastore.Put(c, key, &propToWrite)
		if e != nil {
			log.Errorf(c, "datastore error: "+e.Error())
		}
		//listp = append(listp, p)
	}

	//if len(propsList) < 1 {
	//	res[0] = byte('0')
	//	return (res)
	//}
	/*
		for _, props := range propsList {
			m := make(map[string]interface{})
			for _, prop := range props {
				m[prop.Name] = prop.Value
			}
			vals = append(vals, m)
		}
	*/
	resMap := make(map[string]interface{})
	resMap["payload"] = vals
	b, err := json.Marshal(resMap)
	if err != nil {
		fmt.Println("error in converting json", err.Error())
	}
	return b
}
