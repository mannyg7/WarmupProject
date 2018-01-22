package main

import (
	//"bufio"
	//"bytes"
	//"context"
	//"encoding/csv"
	//"encoding/json"
	//"io"
	//"io/ioutil"
	"net/http"
	//"sort"
	"WarmupProject/database/pkg/datastorehandler"
	"WarmupProject/database/pkg/filehandler"
	"WarmupProject/database/pkg/processhandler"
	"WarmupProject/database/pkg/test"
	//"cloud.google.com/go/storage"
	//"google.golang.org/appengine"
	//"google.golang.org/appengine/blobstore"
	//"google.golang.org/appengine/datastore"
	//"google.golang.org/appengine/file"
	//"google.golang.org/appengine/log"
)

/* function to set up handlers */
func init() {
	http.HandleFunc("/csv", datastorehandler.CsvHandler)
	http.HandleFunc("/process", processhandler.QueryHandler)
	http.HandleFunc("/blob", filehandler.BlobHandler)
	http.HandleFunc("/upload", filehandler.UploadHandler)
	http.HandleFunc("/download/", filehandler.DownloadHandler)
	http.HandleFunc("/test/", test.TestHandler)
}

/* function to add csv file to datastore.
 * This function will read fileName field from JSON in POST request,
 * find corresponding file in Google Storage, parse csv, and store
 * entries to Google Datastore.

func csvHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")

	var datastoreKeys []*datastore.Key
	var datastoreProps []datastore.PropertyList
	c := appengine.NewContext(r)

	/* start read filename from json
	b, err := ioutil.ReadAll(r.Body)
	//log.Infof(c, "request closed")

	if err != nil {
		log.Infof(c, "reading body")
		http.Error(w, err.Error(), 500)
		return
	}

	/* interface{} is essentially object in Java
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

/* start read csv file
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
*/
// func blobHandler(w http.ResponseWriter, r *http.Request) {
// 	ctx := appengine.NewContext(r)
// 	w.Header().Add("Access-Control-Allow-Origin", "*")
// 	w.Header().Add("content-type", "application/json")
// 	w.Header().Set("Access-Control-Allow-Credentials", "true")
// 	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
// 	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")
// 	w.Header().Set("content-type", "application/json")
// 	uploadURL, err := blobstore.UploadURL(ctx, "/upload", nil)
// 	if err != nil {
// 		log.Errorf(ctx, "blobstore error"+err.Error())
// 		fmt.Fprintln(w, "blobstore error"+err.Error())
// 		return
// 	}
// 	// w.Header().Set("Content-Type", "text/html")

// 	// const rootTemplateHTML = `
// 	// <html><body>
// 	// <form action="{{.}}" method="POST" enctype="multipart/form-data">
// 	// Upload File: <input type="file" name="file"><br>
// 	// <input type="submit" name="submit" value="Submit">
// 	// </form>
// 	// </body></html>
// 	// `

// 	// var rootTemplate = template.Must(template.New("root").Parse(rootTemplateHTML))

// 	// err = rootTemplate.Execute(w, uploadURL)
// 	// if err != nil {
// 	// 	log.Errorf(ctx, "%v", err)
// 	// }

// 	resMap := make(map[string]interface{})
// 	resMap["uploadURL"] = uploadURL
// 	b, err := json.Marshal(resMap)

// 	w.Write(b)
// }

// func uploadHandler(w http.ResponseWriter, r *http.Request) {
// 	ctx := appengine.NewContext(r)
// 	log.Debugf(ctx, "upload handler invoked")
// 	blobs, _, err := blobstore.ParseUpload(r)
// 	log.Debugf(ctx, "blobs handled")
// 	if err != nil {
// 		log.Errorf(ctx, "blobstore error"+err.Error())
// 		fmt.Fprintln(w, "blobstore error"+err.Error())
// 		return
// 	}
// 	file := blobs["file"]
// 	if len(file) == 0 {
// 		log.Errorf(ctx, "no file uploaded")
// 		http.Redirect(w, r, "/", http.StatusFound)
// 		return
// 	}
// 	blobKey := file[0].BlobKey
// 	blobReader := blobstore.NewReader(ctx, blobKey)
// 	log.Debugf(ctx, "blobs reader built")
// 	slurp, err := ioutil.ReadAll(blobReader)
// 	log.Debugf(ctx, "blobs data read")

// 	data := string(slurp[:])

// 	readCSV2Datastore(ctx, data, "tvs2")

// 	// w.Write(slurp)
// 	fmt.Fprintln(w, data)
// 	log.Debugf(ctx, data)

// }

// func downloadHandler(w http.ResponseWriter, r *http.Request) {
// 	c := appengine.NewContext(r)

// 	/* start read filename from json */
// 	b, err := ioutil.ReadAll(r.Body)

// 	if err != nil {
// 		log.Infof(c, "reading body")
// 		http.Error(w, err.Error(), 500)
// 		return
// 	}

// 	/* interface{} is essentially object in Java */
// 	var f interface{}
// 	// store the json object mapping into f
// 	err = json.Unmarshal(b, &f)
// 	if err != nil {
// 		log.Debugf(c, string(b))
// 		log.Infof(c, "marshalling: "+err.Error())
// 		http.Error(w, err.Error(), 500)
// 		return
// 	}

// 	m := asMap(f)

// 	entityName := asString(m["entityName"])

// 	w.Header().Add("Access-Control-Allow-Origin", "*")
// 	w.Header().Add("content-type", "application/json")
// 	w.Header().Set("Access-Control-Allow-Credentials", "true")
// 	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
// 	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")
// 	w.Header().Set("Content-Type", "text/csv") // setting the content type header to text/csv
// 	w.Header().Set("Content-Disposition", "attachment;filename=TheCSVFileName.csv")

// 	br := &bytes.Buffer{}   // creates IO Writer
// 	wr := csv.NewWriter(br) // creates a csv writer that uses the io buffer.

// 	q := datastore.NewQuery(entityName)

// 	iter := q.Run(c)

// 	var key2index map[string]int
// 	var csvKeys []string

// 	for {
// 		var p datastore.PropertyList

// 		_, err := iter.Next(&p)

// 		if err == datastore.Done {
// 			break // No further entities match the query.
// 		}
// 		if err != nil {
// 			break
// 		}

// 		if len(key2index) == 0 {
// 			key2index = mapKey2Index(p)
// 		}

// 		if len(csvKeys) == 0 {
// 			csvKeys = getCSVKeys(key2index)
// 			wr.Write(csvKeys)
// 		}

// 		row := propList2String(p, key2index)
// 		wr.Write(row)
// 	}

// 	// for i := 0; i < 100; i++ { // make a loop for 100 rows just for testing purposes
// 	// 	wr.Write(record) // converts array of string to comma seperated values for 1 row.
// 	// }
// 	wr.Flush() // writes the csv writer data to the buffered data io writer(b(bytes.buffer))

// 	w.Write(br.Bytes())
// }

/* function to handle query request from user */
// func queryHandler(w http.ResponseWriter, r *http.Request) {
// 	c := appengine.NewContext(r)

// 	/* headers to allow CORS */
// 	w.Header().Add("Access-Control-Allow-Origin", "*")
// 	w.Header().Add("content-type", "application/json")
// 	w.Header().Set("Access-Control-Allow-Credentials", "true")
// 	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
// 	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")
// 	w.Header().Set("content-type", "application/json")

// 	// Read body from request into variable b
// 	log.Debugf(c, "reading body")
// 	b, err := ioutil.ReadAll(r.Body)
// 	log.Debugf(c, "Finished reading:"+string(b))
// 	defer r.Body.Close()
// 	if err != nil {
// 		log.Errorf(c, "reading body error: "+err.Error())
// 		http.Error(w, err.Error(), 500)
// 		return
// 	}

// 	/* interface{} is essentially object in Java */
// 	var f interface{}
// 	// store the json object mapping into f
// 	err = json.Unmarshal(b, &f)
// 	log.Debugf(c, "finished unmarshalling")
// 	if err != nil {
// 		log.Errorf(c, "marshalling: "+err.Error())
// 		http.Error(w, err.Error(), 500)
// 		return
// 	}

// 	/*{
// 		columns: [columnName1, columnName2...],
// 		entity: entityName,
// 		filterCond: [],
// 		filterVal: [],
// 		order: orderRule,
// 		limit: limitNumber
// 	}
// 	*/
// 	// unpack json into a map
// 	var m map[string]interface{}
// 	m = asMap(f)
// 	var entityName string
// 	if entity, ok := m["entity"]; ok {
// 		entityName = asString(entity)
// 		log.Debugf(c, "entity name finished")
// 	} else {
// 		log.Errorf(c, "missing entityName: "+err.Error())
// 		http.Error(w, err.Error(), 500)
// 		return
// 	}

// 	q := datastore.NewQuery(entityName)
// 	/*
// 		if cols, ok := m["columns"]; ok {
// 			columns := asStringArray(cols)
// 			log.Debugf(c, "columns finished")
// 			q = q.Project(columns...)
// 		}
// 	*/

// 	if filterCond, ok := m["filterCond"]; ok {
// 		if filterVal, ok := m["filterVal"]; ok {
// 			filterConditions := asStringArray(filterCond)
// 			log.Debugf(c, "filter conditions finished")
// 			filterValues := asFloatArray(filterVal)
// 			log.Debugf(c, "filter values finished")
// 			if len(filterConditions) == len(filterValues) {
// 				for i, cond := range filterConditions {
// 					fieldName := strings.Split(cond, " ")[0]
// 					log.Debugf(c, fieldName)
// 					q = q.Order(fieldName)
// 					q = q.Filter(cond, filterValues[i])
// 					log.Debugf(c, cond)
// 					log.Debugf(c, strconv.FormatFloat(filterValues[i], 'f', 5, 64))
// 				}
// 			} else {
// 				log.Errorf(c, "filter condition and filter value length mismatch: "+err.Error())
// 			}
// 		}
// 	}

// 	if odr, ok := m["order"]; ok {
// 		order := asString(odr)
// 		log.Debugf(c, "order finished")
// 		q = q.Order(order)
// 	}

// 	if lmt, ok := m["limit"]; ok {
// 		limit := int(asFloat(lmt))
// 		log.Debugf(c, "limit finished")
// 		q = q.Limit(limit)
// 	}

// 	//var propLists []datastore.PropertyList
// 	iter := q.Run(c)
// 	//fmt.Fprintln(w, q)
// 	//fmt.Fprintln(w, iter)

// 	if err != nil {
// 		log.Errorf(c, "query error: "+err.Error())
// 		http.Error(w, err.Error(), 500)
// 		return
// 	}

// 	if cols, ok := m["columns"]; ok {
// 		columns := asStringArray(cols)
// 		log.Debugf(c, "columns finished")
// 		if n, ok := m["entityName"]; ok {
// 			res := saveJSONResponse(c, iter, columns, asString(n))
// 			w.Write(res)
// 		}
// 	} else {
// 		var defaultcols []string
// 		if n, ok := m["entityName"]; ok {
// 			res := saveJSONResponse(c, iter, defaultcols, asString(n))
// 			w.Write(res)
// 		}
// 	}
// }

// // /* read CSV data and store to datastore */
// // func readCSV2Datastore(c context.Context, data string, entityName string) {
// // 	var datastoreKeys []*datastore.Key
// // 	var datastoreProps []datastore.PropertyList

// // 	br := bufio.NewReader(strings.NewReader(data))
// // 	//go csvHelper(r, data, entityName)
// // 	//t := taskqueue.NewPOSTTask("/csvhandler", url.Values())

// // 	/* HACK: assume CSV format to be
// // 	 * #comment
// // 	 * #comment
// // 	 * #key1 key2 key3...
// // 	 */
// // 	br.ReadLine()
// // 	br.ReadLine()

// // 	reader := csv.NewReader(br)
// // 	reader.Comment = '*'
// // 	keys, keyerr := reader.Read()
// // 	// HACK: remove the # in front of first key
// // 	keys[0] = strings.Split(keys[0], "#")[1]
// // 	if keyerr != nil {
// // 		println(keyerr.Error())
// // 	}
// // 	count := 0
// // 	prevCount := 0
// // 	for {
// // 		count = count + 1
// // 		var props datastore.PropertyList
// // 		vals, err := reader.Read()

// // 		if err == io.EOF {
// // 			datastore.PutMulti(c, datastoreKeys[prevCount:], datastoreProps[prevCount:])
// // 			break
// // 		}

// // 		if err != nil {
// // 			log.Errorf(c, "csv read error %s", err.Error())
// // 			break
// // 		}

// // 		for i, v := range vals {
// // 			k := asString(keys[i])
// // 			if f, ferr := strconv.ParseFloat(v, 64); ferr == nil {
// // 				props = append(props, datastore.Property{Name: k, Value: f})
// // 			} else {
// // 				props = append(props, datastore.Property{Name: k, Value: v})
// // 			}
// // 		}
// // 		//fmt.Fprintln(w, props)
// // 		// TODO： multi-add
// // 		key := datastore.NewIncompleteKey(c, entityName, nil)
// // 		datastoreKeys = append(datastoreKeys, key)
// // 		datastoreProps = append(datastoreProps, props)
// // 		if count%300 == 0 {
// // 			log.Infof(c, strconv.Itoa(count))
// // 			log.Infof(c, strconv.Itoa(len(datastoreKeys)))
// // 			datastore.PutMulti(c, datastoreKeys[prevCount:count], datastoreProps[prevCount:count])
// // 			prevCount = count
// // 			// if storeerror != nil {
// // 			// 	log.Infof(c, storeerror.Error())
// // 			// 	http.Error(w, storeerror.Error(), 500)
// // 			// }

// // 		}

// // 		//_, err = datastore.Put(c, key, &props)
// // 		if err != nil {
// // 			log.Errorf(c, "Datastore Error"+err.Error())
// // 		}
// // 	}
// // }

// /* helper function to convert Property array to json object */
// func saveJSONResponse(c context.Context, iter *datastore.Iterator, cols []string, entName string) []byte {

// 	var vals []map[string]interface{}
// 	//var res []byte
// 	//fmt.Fprintln(w, cols)

// 	for {
// 		var p datastore.PropertyList
// 		var propToWrite datastore.PropertyList
// 		//var props []datastore.Property
// 		_, err := iter.Next(&p)

// 		if err == datastore.Done {
// 			//log.Errorf(c, "datastore Done")
// 			break // No further entities match the query.
// 		}
// 		if err != nil {
// 			//log.Errorf(c, "fetching next Person: %v", err)
// 			break
// 		}
// 		//fmt.Fprintln(w, p)
// 		m := make(map[string]interface{})

// 		//HACK: PROJECTION
// 		for _, prop := range p {

// 			if len(cols) != 0 {

// 				if in(prop.Name, cols) {
// 					propToWrite = append(propToWrite, prop)
// 					//fmt.Fprintln(w, i, cols[i])
// 					m[prop.Name] = prop.Value
// 					//fmt.Fprintln(w, prop.Value)
// 				}
// 			} else {
// 				m[prop.Name] = prop.Value
// 			}
// 		}
// 		// save processed data to datastore
// 		vals = append(vals, m)
// 		key := datastore.NewIncompleteKey(c, entName, nil)
// 		_, e := datastore.Put(c, key, &propToWrite)
// 		if e != nil {
// 			log.Errorf(c, "datastore error: "+e.Error())
// 		}
// 		//listp = append(listp, p)
// 	}

// 	//if len(propsList) < 1 {
// 	//	res[0] = byte('0')
// 	//	return (res)
// 	//}
// 	/*
// 		for _, props := range propsList {
// 			m := make(map[string]interface{})
// 			for _, prop := range props {
// 				m[prop.Name] = prop.Value
// 			}
// 			vals = append(vals, m)
// 		}
// 	*/
// 	resMap := make(map[string]interface{})
// 	resMap["payload"] = vals
// 	b, err := json.Marshal(resMap)
// 	if err != nil {
// 		fmt.Println("error in converting json", err.Error())
// 	}
// 	return b
// }
