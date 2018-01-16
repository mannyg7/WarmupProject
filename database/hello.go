package guestbook

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

// [START greeting_struct]
type Greeting struct {
	Author  string
	Content string
	Date    time.Time
}

// [END greeting_struct]

type DataEntries struct {
	EntityName string
	Payloads   [][]SingleEntry
}

type SingleEntry struct {
	Key   string
	Value string
}

// [END greeting_struct]

func init() {
	http.HandleFunc("/", root)
	http.HandleFunc("/sign", sign)
	http.HandleFunc("/json", jsonHandler)
}

// guestbookKey returns the key used for all guestbook entries.
func guestbookKey(c context.Context) *datastore.Key {
	// The string "default_guestbook" here could be varied to have multiple guestbooks.
	return datastore.NewKey(c, "Guestbook", "default_guestbook", 0, nil)
}

// [START func_root]
func root(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	// Ancestor queries, as shown here, are strongly consistent with the High
	// Replication Datastore. Queries that span entity groups are eventually
	// consistent. If we omitted the .Ancestor from this query there would be
	// a slight chance that Greeting that had just been written would not
	// show up in a query.
	// [START query]
	q := datastore.NewQuery("Greeting").Ancestor(guestbookKey(c)).Order("-Date").Limit(10)
	// [END query]
	// [START getall]
	greetings := make([]Greeting, 0, 10)
	if _, err := q.GetAll(c, &greetings); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// [END getall]
	if err := guestbookTemplate.Execute(w, greetings); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// [END func_root]

var guestbookTemplate = template.Must(template.New("book").Parse(`
<html>
  <head>
    <title>Go Guestbook</title>
  </head>
  <body>
    {{range .}}
      {{with .Author}}
        <p><b>{{.}}</b> wrote:</p>
      {{else}}
        <p>An anonymous person wrote:</p>
      {{end}}
      <pre>{{.Content}}</pre>
    {{end}}
    <form action="/sign" method="post">
			<div><textarea name="content" rows="3" cols="60"></textarea></div>
			<div><textarea name="author" rows="1" cols="60"></textarea></div>
			<br>
			<div>
				[Field 1] Entity Name: <input type="text" name="entname"><br>
				[Field 1] key: <input type="text" name="key1"> value: <input type="text" name="val1"><br>
				[Field 2] key: <input type="text" name="key2"> value: <input type="text" name="val2"><br>
				[Field 3] key: <input type="text" name="key3"> value: <input type="text" name="val3"><br>
			</div>
			<br>
      <div><input type="submit" value="Sign Guestbook"></div>
    </form>
  </body>
</html>
`))

// [START func_sign]
func sign(w http.ResponseWriter, r *http.Request) {
	// [START new_context]
	c := appengine.NewContext(r)
	// [END new_context]
	g := Greeting{
		Content: r.FormValue("content"),
		Author:  r.FormValue("author"),
		Date:    time.Now(),
	}
	// [START if_user]
	/* "user" is an imported package */
	// We set the same parent key on every Greeting entity to ensure each Greeting
	// is in the same entity group. Queries across the single entity group
	// will be consistent. However, the write rate to a single entity group
	// should be limited to ~1/second.
	key := datastore.NewIncompleteKey(c, "Greeting", guestbookKey(c))
	_, err := datastore.Put(c, key, &g)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	/* test dynamic entity */
	var props datastore.PropertyList
	props = append(props, datastore.Property{Name: r.FormValue("key1"), Value: r.FormValue("val1")})
	props = append(props, datastore.Property{Name: r.FormValue("key2"), Value: r.FormValue("val2")})
	props = append(props, datastore.Property{Name: r.FormValue("key3"), Value: r.FormValue("val3")})

	k := datastore.NewIncompleteKey(c, r.FormValue("entname"), nil)
	datastore.Put(c, k, &props)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusFound)
	// [END if_user]
}

// [END func_sign]

func jsonHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	// Read body
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	var f interface{}
	// store the json object mapping into f
	err = json.Unmarshal(b, &f)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// unpack json into a map
	m := asMap(f)
	// headers := m["headers"].(map[string]interface{})
	body := asMap(m["body"])
	entityName := asString(body["entityName"])
	log.Infof(c, entityName)
	payload := asArray(body["payload"])

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// loop through each entry in payload
	for _, u := range payload {
		var props datastore.PropertyList

		kv := asMap(u)
		// loop through each key-value pair in each entry
		for k, v := range kv {
			switch v.(type) {
			case string:
				log.Infof(c, k)
				log.Infof(c, asString(v))
				props = append(props, datastore.Property{Name: k, Value: asString(v)})
			case float64:
				log.Infof(c, k)
				log.Infof(c, strconv.FormatFloat(asFloat(v), 'f', 5, 64))
				props = append(props, datastore.Property{Name: k, Value: asFloat(v)})
			case int:
				log.Infof(c, k)
				log.Infof(c, strconv.FormatInt(v.(int64), 10))
				props = append(props, datastore.Property{Name: k, Value: asInt(v)})
			default:
				fmt.Println(k, "is of a type I don't know how to handle")
			}
		}
		key := datastore.NewIncompleteKey(c, entityName, nil)
		datastore.Put(c, key, &props)
	}

	output, err := json.Marshal(f)
	if err != nil {
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
