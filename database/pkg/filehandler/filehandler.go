package filehandler

import (
	//"bufio"
	"bytes"
	//"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	//"io"
	"io/ioutil"
	"net/http"
	//"sort"
	"WarmupProject/database/pkg/datastorehandler"
	"WarmupProject/database/pkg/helper"
	"strconv"
	//"strings"

	//"cloud.google.com/go/storage"
	"google.golang.org/appengine"
	"google.golang.org/appengine/blobstore"
	"google.golang.org/appengine/datastore"
	//"google.golang.org/appengine/file"
	"google.golang.org/appengine/log"
)

func BlobHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("content-type", "application/json")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")
	w.Header().Set("content-type", "application/json")
	uploadURL, err := blobstore.UploadURL(ctx, "/upload", nil)
	if err != nil {
		log.Errorf(ctx, "blobstore error"+err.Error())
		fmt.Fprintln(w, "blobstore error"+err.Error())
		return
	}
	// w.Header().Set("Content-Type", "text/html")

	// const rootTemplateHTML = `
	// <html><body>
	// <form action="{{.}}" method="POST" enctype="multipart/form-data">
	// Upload File: <input type="file" name="file"><br>
	// <input type="submit" name="submit" value="Submit">
	// </form>
	// </body></html>
	// `

	// var rootTemplate = template.Must(template.New("root").Parse(rootTemplateHTML))

	// err = rootTemplate.Execute(w, uploadURL)
	// if err != nil {
	// 	log.Errorf(ctx, "%v", err)
	// }

	resMap := make(map[string]interface{})
	resMap["uploadURL"] = uploadURL
	b, err := json.Marshal(resMap)

	w.Write(b)
}

func UploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	log.Debugf(ctx, "upload handler invoked")
	blobs, _, err := blobstore.ParseUpload(r)
	log.Debugf(ctx, "blobs handled")
	if err != nil {
		log.Errorf(ctx, "blobstore error"+err.Error())
		fmt.Fprintln(w, "blobstore error"+err.Error())
		return
	}
	file := blobs["file"]
	if len(file) == 0 {
		log.Errorf(ctx, "no file uploaded")
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	blobKey := file[0].BlobKey
	blobReader := blobstore.NewReader(ctx, blobKey)
	log.Debugf(ctx, "blobs reader built")
	slurp, err := ioutil.ReadAll(blobReader)
	log.Debugf(ctx, "blobs data read")

	data := string(slurp[:])

	datastorehandler.ReadCSV2Datastore(ctx, data, "tvs2")

	// w.Write(slurp)
	fmt.Fprintln(w, data)
	log.Debugf(ctx, data)

}

/* function to handle table download request */
func DownloadHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	entityName := strings.TrimPrefix(r.URL.Path, "/download/")

	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("content-type", "application/json")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")
	w.Header().Set("Content-Type", "text/csv") // setting the content type header to text/csv
	w.Header().Set("Content-Disposition", "attachment;filename=TheCSVFileName.csv")

	br := &bytes.Buffer{}   // creates IO Writer
	wr := csv.NewWriter(br) // creates a csv writer that uses the io buffer.

	q := datastore.NewQuery(entityName)

	iter := q.Run(c)

	var key2index map[string]int
	var csvKeys []string

	for {
		var p datastore.PropertyList

		_, err := iter.Next(&p)

		if err == datastore.Done {
			break // No further entities match the query.
		}
		if err != nil {
			break
		}

		if len(key2index) == 0 {
			key2index = mapKey2Index(p)
		}

		if len(csvKeys) == 0 {
			csvKeys = getCSVKeys(key2index)
			wr.Write(csvKeys)
		}

		row := propList2String(p, key2index)
		wr.Write(row)
	}

	// for i := 0; i < 100; i++ { // make a loop for 100 rows just for testing purposes
	// 	wr.Write(record) // converts array of string to comma seperated values for 1 row.
	// }
	wr.Flush() // writes the csv writer data to the buffered data io writer(b(bytes.buffer))

	w.Write(br.Bytes())
}

/* given a property list, return a mapping from key name to index */
func mapKey2Index(plist datastore.PropertyList) map[string]int {
	m := make(map[string]int)
	for i, prop := range plist {
		m[prop.Name] = i
	}
	return m
}

/* given a map from key name to index, return an array of string indicating CSV keys */
func getCSVKeys(m map[string]int) []string {
	record := make([]string, len(m))
	for k, v := range m {
		record[v] = k
	}
	return record
}

/* given a property list and a mapping from key name to index, return an array of string */
func propList2String(plist datastore.PropertyList, m map[string]int) []string {
	row := make([]string, len(m))

	for _, prop := range plist {
		v := prop.Value
		_, ok := v.(string)
		if !ok {
			_, ok = v.(int64)
			if !ok {
				row[m[prop.Name]] = strconv.FormatFloat(helper.AsFloat(v), 'f', 6, 64)
			} else {
				row[m[prop.Name]] = strconv.FormatInt(v.(int64), 10)
			}
		} else {
			row[m[prop.Name]] = helper.AsString(prop.Value)
		}

	}

	return row
}
