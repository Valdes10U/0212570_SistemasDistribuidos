package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
)

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var records []Record

func writeHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Record struct {
			Value string `json:"value"`
		} `json:"record"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	decodedValue, err := base64.StdEncoding.DecodeString(req.Record.Value)
	if err != nil {
		http.Error(w, "invalid base64 value", http.StatusBadRequest)
		return
	}

	record := Record{
		Value:  decodedValue,
		Offset: uint64(len(records)),
	}
	records = append(records, record)

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
	offsetStr := r.URL.Query().Get("offset")
	if offsetStr == "" {
		http.Error(w, "missing offset parameter", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset parameter", http.StatusBadRequest)
		return
	}

	if offset >= uint64(len(records)) {
		http.Error(w, "offset out of range", http.StatusBadRequest)
		return
	}

	record := records[offset]

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
