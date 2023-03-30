package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"atomkv"
)

var db *atomkv.Bitcask

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	var err error
	db, err = atomkv.Open("atomkv.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Load(); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/keys", handleKeys)
	http.HandleFunc("/compact", handleCompact)

	log.Printf("atomkv server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := db.Set(req.Key, req.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key parameter", http.StatusBadRequest)
		return
	}

	val, err := db.Get(key)
	if err != nil {
		if err == atomkv.ErrKeyNotFound {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, val)
}

func handleKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.Keys()
	json.NewEncoder(w).Encode(keys)
}

func handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := db.Compact(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "OK")
}
