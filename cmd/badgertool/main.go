package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
)

func main() {
	var db *badger.DB
	var err error
	dbPath := flag.String("db", "", "database directory")
	level := flag.String("l", "error", "log level")
	flag.Parse()

	if logLevel, err := logrus.ParseLevel(*level); err != nil {
		logrus.Fatal(err)
	} else {
		logrus.SetLevel(logLevel)
	}

	keys := []string{}
	for _, v := range flag.Args() {
		v = "\"" + v + "\""
		s, err := strconv.Unquote(v)
		if err != nil {
			logrus.Fatalf("invalid key %v", v)
		}
		keys = append(keys, s)
	}

	opts := badger.DefaultOptions(*dbPath)
	opts.Logger = logrus.StandardLogger()
	db, err = badger.Open(opts)
	if err != nil {
		logrus.Fatal(err)
	}
	defer db.Close()

	db.View(func(txn *badger.Txn) error {
		// try get if len(keys) == 1
		if len(keys) == 1 {
			v, err := txn.Get([]byte(keys[0]))
			if err != badger.ErrKeyNotFound {
				if err != nil {
					logrus.Fatal(err)
				}
				v.Value(func(val []byte) error {
					fmt.Println(strconv.Quote(string(val)))
					return nil
				})
				return nil
			}
		}
		// traversal to match all keys
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Rewind()
		for ; it.Valid(); it.Next() {
			if matchKeys(string(it.Item().Key()), keys) {
				fmt.Print(strconv.Quote(string(it.Item().Key())), " ")
				if err := it.Item().Value(func(val []byte) error {
					fmt.Println(strconv.Quote(string(val)))
					return nil
				}); err != nil {
					logrus.Fatal(err)
				}
			}
		}
		return nil
	})
}

func matchKeys(s string, keys []string) bool {
	for _, key := range keys {
		if !strings.Contains(s, key) {
			return false
		}
	}
	return true
}
