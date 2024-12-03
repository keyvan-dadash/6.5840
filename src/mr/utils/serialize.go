package utils

import "encoding/json"

func ToJson(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func FromJson(data string, strucut interface{}) {
	if err := json.Unmarshal([]byte(data), strucut); err != nil {
		panic(err)
	}
}
