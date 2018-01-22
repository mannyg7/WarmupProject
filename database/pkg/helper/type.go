package helper

import (
	"fmt"
	"strconv"
)

/* cast functions to cast object to concrete types */
func AsMap(o interface{}) map[string]interface{} {
	return o.(map[string]interface{})
}

func AsArray(o interface{}) []interface{} {
	return o.([]interface{})
}

func AsStringArray(o interface{}) []string {
	t := AsArray(o)
	s := make([]string, len(t))
	for i, v := range t {
		s[i] = fmt.Sprint(v)
	}
	return s
}

func AsFloatArray(o interface{}) []float64 {
	t := AsArray(o)
	s := make([]float64, len(t))
	for i, v := range t {
		s[i] = AsFloat(v)
	}
	return s
}

func AsInt(o interface{}) int {
	return o.(int)
}

func AsFloat(o interface{}) float64 {
	return o.(float64)
}

func AsString(o interface{}) string {
	return o.(string)
}

func AsBool(o interface{}) bool {
	return o.(bool)
}

func In(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func Str2int(s string) int {
	i, _ := strconv.ParseInt(s, 10, 64)
	return int(i)
}

func Str2float(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
