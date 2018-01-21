package hello

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	//"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/appengine"
	"google.golang.org/appengine/blobstore"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/file"
	"google.golang.org/appengine/log"
)

/* function to set up handlers */
func init() {
	http.HandleFunc("/json", jsonHandler)
	http.HandleFunc("/csv", csvHandler)
	http.HandleFunc("/query", queryTest)
	http.HandleFunc("/process", queryHandler)
}

/* function to add csv file to datastore.
 * This function will read fileName field from JSON in POST request,
 * find corresponding file in Google Storage, parse csv, and store
 * entries to Google Datastore.
**/
func csvHandler(w http.ResponseWriter, r *http.Request) {
	var datastoreKeys []*datastore.Key
	var datastoreProps []datastore.PropertyList
	c := appengine.NewContext(r)

	/* start read filename from json */
	b, err := ioutil.ReadAll(r.Body)
	//log.Infof(c, "request closed")

	if err != nil {
		log.Infof(c, "reading body")
		http.Error(w, err.Error(), 500)
		return
	}

	/* interface{} is essentially object in Java */
	var f interface{}
	// store the json object mapping into f
	err = json.Unmarshal(b, &f)
	if err != nil {
		log.Debugf(c, string(b))
		log.Infof(c, "marshalling: "+err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	m := asMap(f)
	fileName := asString(m["fileName"])
	entityName := asString(m["entityName"])
	/* end read filename from json */

	/* start read csv file */
	// data := readFile(c, fileName)
	data := readBlob(c, fileName)
	//data := "a"

	if data == "err" {
		log.Errorf(c, "Google Storage file read failed\n")
		return
	}

	br := bufio.NewReader(strings.NewReader(data))
	//go csvHelper(r, data, entityName)
	//t := taskqueue.NewPOSTTask("/csvhandler", url.Values())
	defer r.Body.Close()
	/* HACK: assume CSV format to be
	 * #comment
	 * #comment
	 * #key1 key2 key3...
	 */
	br.ReadLine()
	br.ReadLine()

	reader := csv.NewReader(br)
	reader.Comment = '*'
	keys, keyerr := reader.Read()
	// HACK: remove the # in front of first key
	keys[0] = strings.Split(keys[0], "#")[1]
	if keyerr != nil {
		println(keyerr.Error())
	}
	count := 0
	prevCount := 0
	for {
		count = count + 1
		var props datastore.PropertyList
		vals, err := reader.Read()

		if err == io.EOF {
			datastore.PutMulti(c, datastoreKeys[prevCount:], datastoreProps[prevCount:])
			break
		}

		if err != nil {
			log.Errorf(c, "csv read error %s", err.Error())
			break
		}

		for i, v := range vals {
			k := asString(keys[i])
			if f, ferr := strconv.ParseFloat(v, 64); ferr == nil {
				props = append(props, datastore.Property{Name: k, Value: f})
			} else {
				props = append(props, datastore.Property{Name: k, Value: v})
			}
		}
		//fmt.Fprintln(w, props)
		// TODO： multi-add
		key := datastore.NewIncompleteKey(c, entityName, nil)
		datastoreKeys = append(datastoreKeys, key)
		datastoreProps = append(datastoreProps, props)
		if count%300 == 0 {
			log.Infof(c, strconv.Itoa(count))
			log.Infof(c, strconv.Itoa(len(datastoreKeys)))
			_, storeerror := datastore.PutMulti(c, datastoreKeys[prevCount:count], datastoreProps[prevCount:count])
			prevCount = count
			if storeerror != nil {
				log.Infof(c, storeerror.Error())
				http.Error(w, storeerror.Error(), 500)
			}

		}

		//_, err = datastore.Put(c, key, &props)
		if err != nil {
			log.Errorf(c, "Datastore Error"+err.Error())
		}
	}
	fmt.Fprintln(w, "operation completed")

}

func csvHandlerStatic(w http.ResponseWriter, r *http.Request) {
	var datastoreKeys []*datastore.Key
	var datastoreProps []datastore.PropertyList
	c := appengine.NewContext(r)

	/* start read filename from json */
	b, err := ioutil.ReadAll(r.Body)
	//log.Infof(c, "request closed")

	if err != nil {
		log.Infof(c, "reading body")
		http.Error(w, err.Error(), 500)
		return
	}

	/* interface{} is essentially object in Java */
	var f interface{}
	// store the json object mapping into f
	err = json.Unmarshal(b, &f)
	if err != nil {
		log.Infof(c, "marshalling: "+err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	m := asMap(f)
	fileName := asString(m["fileName"])
	entityName := asString(m["entityName"])
	/* end read filename from json */

	/* start read csv file */
	// data := readFile(c, fileName)
	data := readBlob(c, fileName)
	//data := "a"

	if data == "err" {
		log.Errorf(c, "Google Storage file read failed\n")
		return
	}

	br := bufio.NewReader(strings.NewReader(data))
	//go csvHelper(r, data, entityName)
	//t := taskqueue.NewPOSTTask("/csvhandler", url.Values())
	defer r.Body.Close()
	/* HACK: assume CSV format to be
	 * #comment
	 * #comment
	 * #key1 key2 key3...
	 */
	br.ReadLine()
	br.ReadLine()

	reader := csv.NewReader(br)
	reader.Comment = '*'
	keys, keyerr := reader.Read()
	// HACK: remove the # in front of first key
	keys[0] = strings.Split(keys[0], "#")[1]
	if keyerr != nil {
		println(keyerr.Error())
	}
	count := 0
	for {
		count = count + 1
		var props datastore.PropertyList
		vals, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Errorf(c, "csv read error %s", err.Error())
			break
		}

		for i, v := range vals {
			k := asString(keys[i])
			if i, ierr := strconv.ParseInt(v, 10, 64); ierr == nil {
				props = append(props, datastore.Property{Name: k, Value: i})
			} else if f, ferr := strconv.ParseFloat(v, 64); ferr == nil {
				props = append(props, datastore.Property{Name: k, Value: f})
			} else {
				props = append(props, datastore.Property{Name: k, Value: v})
			}
		}
		//fmt.Fprintln(w, props)
		// TODO： multi-add
		key := datastore.NewIncompleteKey(c, entityName, nil)
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

		//_, err = datastore.Put(c, key, &props)
		if err != nil {
			log.Errorf(c, "Datastore Error"+err.Error())
		}
	}
	fmt.Fprintln(w, "operation completed")

}

func queryTest(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	// q := datastore.NewQuery("test-csv-types").Order("-b").Limit(2) //.Project("#a", "b") //.Filter("BASE>", 10.0).Order("BASE")
	q := datastore.NewQuery("tvs").Filter("LAT>=", 10.0).Order("BASE")
	t := q.Run(c)
	var listp []datastore.PropertyList
	for {
		var p datastore.PropertyList
		//var props []datastore.Property
		_, err := t.Next(&p)
		if err == datastore.Done {
			log.Errorf(c, "datastore Done")
			break // No further entities match the query.
		}
		if err != nil {
			log.Errorf(c, "fetching next Person: %v", err)
			break
		}
		listp = append(listp, p)
	}
	//log.Debugf(c, string(saveJSONResponse(listp)))
	//fmt.Fprintln(w, listp)
	//
	//fmt.Fprintln(w, q)
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

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
	m = asMap(f)
	var entityName string
	if entity, ok := m["entity"]; ok {
		entityName = asString(entity)
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
			filterConditions := asStringArray(filterCond)
			log.Debugf(c, "filter conditions finished")
			filterValues := asFloatArray(filterVal)
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
		order := asString(odr)
		log.Debugf(c, "order finished")
		q = q.Order(order)
	}

	if lmt, ok := m["limit"]; ok {
		limit := int(asFloat(lmt))
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
		columns := asStringArray(cols)
		log.Debugf(c, "columns finished")
		res := saveJSONResponse(iter, columns)
		w.Header().Set("content-type", "application/json")
		w.Write(res)
	} else {
		var defaultcols []string
		res := saveJSONResponse(iter, defaultcols)
		w.Header().Set("content-type", "application/json")
		w.Write(res)
	}
}

/* function to handle json requests DEPRECATED RN */
func jsonHandler(w http.ResponseWriter, r *http.Request) {
	//fmt.Println(r)
	c := appengine.NewContext(r)
	log.Infof(c, "request")

	// Read body from request into variable b
	b, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
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
		log.Errorf(c, "marshalling: "+err.Error())
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

/* helper function to read file from Google Storage into blob */
func readBlob(c context.Context, fileName string) string {
	/* read file from Google Storage*/

	// initialization
	bucketName, err := file.DefaultBucketName(c)
	if err != nil {
		log.Errorf(c, "failed to get default GCS bucket name: %v", err)
	}

	client, err := storage.NewClient(c)
	if err != nil {
		log.Errorf(c, "failed to create client: %v", err)
		return "err"
	}
	defer client.Close()

	// bucket := client.Bucket(bucketName)

	// rc, err := bucket.Object(fileName).NewReader(c)
	path := "/gs/" + bucketName + "/" + fileName
	blobKey, err := blobstore.BlobKeyForFile(c, path)
	blobReader := blobstore.NewReader(c, blobKey)
	slurp, err := ioutil.ReadAll(blobReader)

	if err != nil {
		log.Errorf(c, "readFile: unable to open file from bucket %q, file %q: %v", bucketName, fileName, err)
		return "err"
	}

	// defer rc.Close()

	// slurp, err := ioutil.ReadAll(rc)
	if err != nil {
		log.Errorf(c, "readFile: unable to read data from bucket %q, file %q: %v", bucketName, fileName, err)
		return "err"
	}

	data := string(slurp[:])
	return data
}

/* helper function to convert Property array to json object */
func saveJSONResponse(iter *datastore.Iterator, cols []string) []byte {

	var vals []map[string]interface{}
	//var res []byte
	//fmt.Fprintln(w, cols)

	for {
		var p datastore.PropertyList
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

				if in(prop.Name, cols) {
					//fmt.Fprintln(w, i, cols[i])
					m[prop.Name] = prop.Value
					//fmt.Fprintln(w, prop.Value)
				}
			} else {
				m[prop.Name] = prop.Value
			}
		}
		vals = append(vals, m)
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

/* cast functions to cast object to concrete types */
func asMap(o interface{}) map[string]interface{} {
	return o.(map[string]interface{})
}

func asArray(o interface{}) []interface{} {
	return o.([]interface{})
}

func asStringArray(o interface{}) []string {
	t := asArray(o)
	s := make([]string, len(t))
	for i, v := range t {
		s[i] = fmt.Sprint(v)
	}
	return s
}

func asFloatArray(o interface{}) []float64 {
	t := asArray(o)
	s := make([]float64, len(t))
	for i, v := range t {
		s[i] = asFloat(v)
	}
	return s
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

func asBool(o interface{}) bool {
	return o.(bool)
}

func in(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
