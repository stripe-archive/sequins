package main
import (
  "net/http"
  "fmt"
)

func (s *sequins) serveMultiget(w http.ResponseWriter, r *http.Request) {
  err := r.ParseForm()
  if err != nil {
    w.WriteHeader(http.StatusBadRequest)
    w.Write([]byte(fmt.Sprintf("parseForm failed %s", err)))
    return
  }

  dbName := r.Form.Get("db")
  if dbName == "" {
    w.WriteHeader(http.StatusBadRequest)
    w.Write([]byte("no db found"))
    return
  }

  s.dbsLock.RLock()
  db := s.dbs[dbName]
  s.dbsLock.RUnlock()

  if db == nil {
    w.WriteHeader(http.StatusNotFound)
    return
  }

  keys := r.Form["key"]
  if len(keys) == 0 {
    w.WriteHeader(http.StatusBadRequest)
    w.Write([]byte("no keys found"))
    return
  }

  db.serveMultiget(w, r, keys)



}
