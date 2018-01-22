package test

import (
	//"bufio"
	//"bytes"
	//"context"
	//"encoding/csv"
	//"encoding/json"
	"fmt"
	//"io"
	//"io/ioutil"
	"net/http"
	//"sort"
	//"cloud.google.com/go/storage"
	//"google.golang.org/appengine"
	//"google.golang.org/appengine/blobstore"
	//"google.golang.org/appengine/datastore"
	//"google.golang.org/appengine/file"
	//"google.golang.org/appengine/log"
)

func TestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Requested-With, X-Session-Id")
	fmt.Fprintln(w, "hello world")
}
