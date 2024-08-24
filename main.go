package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
)

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type Log struct {
	mu      sync.Mutex
	records []Record
}

var log = Log{}

func writeHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Record struct {
			Value string `json:"value"`
		} `json:"record"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	decodedValue, err := base64.StdEncoding.DecodeString(req.Record.Value)
	if err != nil {
		http.Error(w, "invalid base64 value", http.StatusBadRequest)
		return
	}

	log.mu.Lock()
	defer log.mu.Unlock()

	record := Record{
		Value:  decodedValue,
		Offset: uint64(len(log.records)),
	}
	log.records = append(log.records, record)

	res := struct {
		Offset uint64 `json:"offset"`
	}{
		Offset: record.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func readHandler(w http.ResponseWriter, r *http.Request) {
	offsetParam := r.URL.Query().Get("offset")
	offset, err := strconv.ParseUint(offsetParam, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	log.mu.Lock()
	defer log.mu.Unlock()

	if offset >= uint64(len(log.records)) {
		http.Error(w, "offset out of range", http.StatusNotFound)
		return
	}

	record := log.records[offset]

	res := struct {
		Value string `json:"value"`
	}{
		Value: base64.StdEncoding.EncodeToString(record.Value),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func main() {
	http.HandleFunc("/write", writeHandler)
	http.HandleFunc("/read", readHandler)
	http.ListenAndServe(":8080", nil)
}
