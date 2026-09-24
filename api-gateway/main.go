package main

import (
	"fmt"
	"net/http"
)

func main() {
    http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"status": "API Gateway is running"}`))
    })

    fmt.Println("API Gateway listening on :8081...")
    if err := http.ListenAndServe(":8081", nil); err != nil {
        panic(err)
    }
}