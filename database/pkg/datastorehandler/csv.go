package datastorehandler

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
	//"WarmupProject/database/pkg/filehandler"
	"WarmupProject/database/pkg/helper"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/appengine"
	"google.golang.org/appengine/blobstore"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/file"
	"google.golang.org/appengine/log"
)

/* function to add csv file to datastore.
 * This function will read fileName field from JSON in POST request,
 * find corresponding file in Google Storage, parse csv, and store
 * entries to Google Datastore.
**/
func CsvHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")

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

	m := helper.AsMap(f)
	fileName := helper.AsString(m["fileName"])
	entityName := helper.AsString(m["entityName"])
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
			k := helper.AsString(keys[i])
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

/* read CSV data and store to datastore */
func ReadCSV2Datastore(c context.Context, data string, entityName string) {
	var datastoreKeys []*datastore.Key
	var datastoreProps []datastore.PropertyList

	br := bufio.NewReader(strings.NewReader(data))
	//go csvHelper(r, data, entityName)
	//t := taskqueue.NewPOSTTask("/csvhandler", url.Values())

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
			k := helper.AsString(keys[i])
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
			datastore.PutMulti(c, datastoreKeys[prevCount:count], datastoreProps[prevCount:count])
			prevCount = count
			// if storeerror != nil {
			// 	log.Infof(c, storeerror.Error())
			// 	http.Error(w, storeerror.Error(), 500)
			// }

		}

		//_, err = datastore.Put(c, key, &props)
		if err != nil {
			log.Errorf(c, "Datastore Error"+err.Error())
		}
	}
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

	path := "/gs/" + bucketName + "/" + fileName
	blobKey, err := blobstore.BlobKeyForFile(c, path)
	blobReader := blobstore.NewReader(c, blobKey)
	slurp, err := ioutil.ReadAll(blobReader)

	if err != nil {
		log.Errorf(c, "readFile: unable to open file from bucket %q, file %q: %v", bucketName, fileName, err)
		return "err"
	}

	if err != nil {
		log.Errorf(c, "readFile: unable to read data from bucket %q, file %q: %v", bucketName, fileName, err)
		return "err"
	}

	data := string(slurp[:])
	return data
}

func ProcessHistogram(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")
	b, err := ioutil.ReadAll(r.Body)
	c := appengine.NewContext(r)
	log.Infof(c, "request received")

	if err != nil {
		//log.Infof(c, "reading body")
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

	m := helper.AsMap(f)
	fileName := helper.AsString(m["fileName"])
	col1 := helper.AsString(m["col1"])
	col2 := helper.AsString(m["col2"])
	bin := helper.Str2float(helper.AsString(m["binSize"]))
	defer r.Body.Close()
	avgMap := makeAvgMap(c, fileName, col1, col2, bin)
	log.Debugf(c, "avgmap completed")

	var res []map[string]float64
	for key, val := range avgMap {
		responseMap := make(map[string]float64)
		responseMap[col1] = key
		responseMap[col2] = val
		res = append(res, responseMap)

		// if len(cols) != 0 {

		// 	if helper.In(prop.Name, cols) {
		// 		propToWrite = append(propToWrite, prop)
		// 		//fmt.Fprintln(w, i, cols[i])
		// 		m[prop.Name] = prop.Value
		// 		//fmt.Fprintln(w, prop.Value)
		// 	}
		// } else {
		// 	m[prop.Name] = prop.Value
		// }
	}
	resMap := make(map[string]interface{})
	resMap["payload"] = res
	response, err := json.Marshal(resMap)
	if err != nil {
		fmt.Println("error in converting json", err.Error())
	}
	log.Debugf(c, "writing response")

	fmt.Fprintln(w, string(response[:]))
	// save processed data to datastore
	// vals = append(vals, m)
	// key := datastore.NewIncompleteKey(c, entName, nil)
	// _, e := datastore.Put(c, key, &propToWrite)
	// if e != nil {
	// 	log.Errorf(c, "datastore error: "+e.Error())
	// }
	/* end read filename from json */

	/* start read csv file */
	// data := readFile(c, fileName)

}

func makeAvgMap(c context.Context, fileName string, col1 string, col2 string, bin float64) map[float64]float64 {
	data := readBlob(c, fileName)
	log.Debugf(c, "blob read ready")

	if data == "err" {
		log.Errorf(c, "Google Storage file read failed\n")
		return nil
	}

	br := bufio.NewReader(strings.NewReader(data))
	//go csvHelper(r, data, entityName)
	//t := taskqueue.NewPOSTTask("/csvhandler", url.Values())
	//
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

	keys[0] = strings.Split(keys[0], "#")[1]
	if keyerr != nil {
		println(keyerr.Error())
	}
	min := -180.0
	max := 180.0
	var bins []float64
	for i := min; i < max; i += bin {
		bins = append(bins, i)
	}
	countMap := make(map[float64]int)
	sumMap := make(map[float64]float64)
	keyMap := make(map[string]int)
	for i, key := range keys {
		keyMap[key] = i
	}
	idx1 := keyMap[col1]
	log.Debugf(c, "start processing")
	//idx2 := keyMap[col2]
	for {
		//count = count + 1
		//var props datastore.PropertyList
		vals, err := reader.Read()

		if err == io.EOF {
			//datastore.PutMulti(c, datastoreKeys[prevCount:], datastoreProps[prevCount:])
			break
		}

		if err != nil {
			//log.Errorf(c, "csv read error %s", err.Error())
			break
		}

		val1 := helper.Str2float(vals[idx1])
		//val2 := helper.Str2float(vals[idx2])
		for _, lowBound := range bins {
			if val1 > lowBound {
				countMap[lowBound] = countMap[lowBound] + 1
				sumMap[lowBound] = sumMap[lowBound] + val1
			}
		}

		// for i, v := range vals {

		// 	// k := helper.AsString(keys[i])
		// 	// if f, ferr := strconv.ParseFloat(v, 64); ferr == nil {
		// 	// 	props = append(props, datastore.Property{Name: k, Value: f})
		// 	// } else {
		// 	// 	props = append(props, datastore.Property{Name: k, Value: v})
		// 	// }
		// }
		//fmt.Fprintln(w, props)
		// TODO： multi-add
		// key := datastore.NewIncompleteKey(c, entityName, nil)
		// datastoreKeys = append(datastoreKeys, key)
		// datastoreProps = append(datastoreProps, props)
		// if count%300 == 0 {
		// 	log.Infof(c, strconv.Itoa(count))
		// 	log.Infof(c, strconv.Itoa(len(datastoreKeys)))
		// 	_, storeerror := datastore.PutMulti(c, datastoreKeys[prevCount:count], datastoreProps[prevCount:count])
		// 	prevCount = count
		// 	if storeerror != nil {
		// 		log.Infof(c, storeerror.Error())
		// 		http.Error(w, storeerror.Error(), 500)
		// 	}

		// }
	}
	log.Debugf(c, "Mapping finished")
	average := make(map[float64]float64)
	for k, v := range sumMap {
		average[k] = v / float64(countMap[k])
	}
	return average

}
