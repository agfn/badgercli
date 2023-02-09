package main

import (
	"errors"
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

	cmd := flag.Arg(0)
	m := map[string]func(_ *badger.DB, _ []string){
		"get": cmdGet,
		"set": cmdSet,
	}
	help := func(_ *badger.DB, _ []string) {
		fmt.Println("badgertool -db [datebase] [get|set] [keys|key] [value]")
	}
	m["help"] = help
	if len(flag.Args()) == 0 {
		help(nil, nil)
	} else {
		opts := badger.DefaultOptions(*dbPath)
		opts.Logger = logrus.StandardLogger()
		if cmd == "get" {
			opts.ReadOnly = true
		}
		db, err = badger.Open(opts)
		if err != nil {
			logrus.Fatal(err)
		}
		defer db.Close()

		m[cmd](db, flag.Args()[1:])
	}
}

func cmdGet(db *badger.DB, args []string) {
	keys := []string{}
	for _, v := range args {
		keys = append(keys, parseQuoteString(v))
	}

	db.View(func(txn *badger.Txn) error {
		// try get if len(keys) == 1
		if len(keys) == 1 {
			v, err := txn.Get([]byte(keys[0]))
			if err != badger.ErrKeyNotFound {
				if err != nil {
					logrus.Fatal(err)
				}
				v.Value(func(val []byte) error {
					fmt.Println(strconv.QuoteToASCII(string(val)))
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
				fmt.Print(strconv.QuoteToASCII(string(it.Item().Key())), " ")
				if err := it.Item().Value(func(val []byte) error {
					fmt.Println(strconv.QuoteToASCII(string(val)))
					return nil
				}); err != nil {
					logrus.Fatal(err)
				}
			}
		}
		return nil
	})
}

func cmdSet(db *badger.DB, args []string) {
	flags := flag.NewFlagSet("set", flag.ExitOnError)
	force := flags.Bool("f", false, "force (set without confirm)")
	flags.Parse(args)

	key := parseQuoteString(flags.Arg(0))
	value := parseQuoteString(flags.Arg(1))
	txn := db.NewTransaction(true)
	doWrite := true
	item, err := txn.Get([]byte(key))
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			logrus.Fatal(err)
		}
	} else {
		orgValue := make([]byte, item.ValueSize())
		if _, err := item.ValueCopy(orgValue); err != nil {
			logrus.Fatal(err)
		}
		fmt.Printf("key already exists: %s %s\n", strconv.QuoteToASCII(key),
			strconv.QuoteToASCII(string(orgValue)))
		if !*force {
			ch := 'n'
			fmt.Printf("continue set? (y/n) ")
			fmt.Scanf("%c", &ch)
			if ch != 'y' {
				doWrite = false
				fmt.Println("canceled")
			}
		}
	}
	if doWrite {
		fmt.Printf("write %s %s\n", strconv.QuoteToASCII(key), strconv.QuoteToASCII(value))
		txn.Set([]byte(key), []byte(value))
		txn.Commit()
	}
}

func matchKeys(s string, keys []string) bool {
	for _, key := range keys {
		if !strings.Contains(s, key) {
			return false
		}
	}
	return true
}

func parseQuoteString(v string) string {
	v = "\"" + v + "\""
	s, err := strconv.Unquote(v)
	if err != nil {
		logrus.Fatalf("invalid key %v", v)
	}
	return s
}
