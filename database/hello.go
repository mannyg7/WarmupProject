package guestbook

import (
	"html/template"
	"net/http"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

// [START greeting_struct]
type Greeting struct {
	Author  string
	Content string
	Date    time.Time
}

// [END greeting_struct]

// [START greeting_struct]
type PersonalInfo struct {
	FirstName string
	LastName  string
	Phone     string
	Email     string
}

// [END greeting_struct]

func init() {
	http.HandleFunc("/", root)
	http.HandleFunc("/sign", sign)
}

// guestbookKey returns the key used for all guestbook entries.
func guestbookKey(c context.Context) *datastore.Key {
	// The string "default_guestbook" here could be varied to have multiple guestbooks.
	return datastore.NewKey(c, "Guestbook", "default_guestbook", 0, nil)
}

func personalInfoKey(c context.Context) *datastore.Key {
	return datastore.NewKey(c, "Infobook", "default_infobook", 0, nil)
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
			<br>
			<div>
				First name: <input type="text" name="firstName"><br>
				Last name: <input type="text" name="lastName"><br>
				Phone: <input type="text" name="phone"><br>
				Email: <input type="text" name="email"><br>
			</div>
			<br>
			<div>
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
	// g := Greeting{
	// 	Content: r.FormValue("content"),
	// 	Date:    time.Now(),
	// }
	// [START if_user]
	/* "user" is an imported package */
	// if u := user.Current(c); u != nil {
	// 	g.Author = u.String()
	// }
	// We set the same parent key on every Greeting entity to ensure each Greeting
	// is in the same entity group. Queries across the single entity group
	// will be consistent. However, the write rate to a single entity group
	// should be limited to ~1/second.
	// key := datastore.NewIncompleteKey(c, "Greeting", guestbookKey(c))
	// _, err := datastore.Put(c, key, &g)

	// if err != nil {
	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
	// 	return
	// }

	// p := PersonalInfo{
	// 	FirstName: r.FormValue("firstName"),
	// 	LastName:  r.FormValue("lastName"),
	// 	Phone:     r.FormValue("phone"),
	// 	Email:     r.FormValue("email"),
	// }

	// key = datastore.NewIncompleteKey(c, "PersonalInfo", personalInfoKey(c))
	// _, err = datastore.Put(c, key, &p)

	// if err != nil {
	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
	// 	return
	// }

	var props datastore.PropertyList
	props = append(props, datastore.Property{Name: r.FormValue("key1"), Value: r.FormValue("val1")})
	// props = append(props, datastore.Property{Name: r.FormValue("key2"), Value: r.FormValue("val2")})
	// props = append(props, datastore.Property{Name: r.FormValue("key3"), Value: r.FormValue("val3")})

	key := datastore.NewIncompleteKey(c, "DynamicEntity", nil)
	_, err := datastore.Put(c, key, &props)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusFound)
	// [END if_user]
}

// [END func_sign]
