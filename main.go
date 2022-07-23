package main

import (
	"cloud.google.com/go/spanner"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

// read data from a csv file
func readData(fileName string) ([][]string, error) {

	f, err := os.Open(fileName)
	if err != nil {
		return [][]string{}, err
	}

	defer f.Close()
	r := csv.NewReader(f)

	records, err := r.ReadAll()
	if err != nil {
		return [][]string{}, err
	}

	return records, nil
}

// Avoid mutation 20k limit
func deleteUsingPartitionedDML(w io.Writer, db string, client *spanner.Client) error {
	ctx := context.Background()

	stmt := spanner.Statement{SQL: "DELETE FROM users WHERE id > 0"}
	rowCount, err := client.PartitionedUpdate(ctx, stmt)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%d record(s) deleted. ", rowCount)
	return nil
}

func insertByMutation(w io.Writer, db string, client *spanner.Client) error {
	ctx := context.Background()

	csvRecords, err := readData("users.csv")

	if err != nil {
		log.Fatal(err)
	}

	var records [][]interface{}

	for _, r := range csvRecords {

		var row []interface{}

		// total 10 colums
		for i := 0; i < 10; i++ {
			// column 2
			if i == 1 {
				row = append(row, r[i])
				continue
			}
			// Convert it to int
			value, _ := strconv.Atoi(r[i])
			row = append(row, value)
		}

		records = append(records, row)
		// fmt.Fprintf(w, "%v", records)
	}

	// interface sample
	//  []interface{}{10001,"Tom",100,2,1,5,0,1,1655774916,1655774916},

	m := []*spanner.Mutation{}
	accountsColumns := []string{"id", "name", "tokens", "type", "color", "coins", "location", "world", "create_time", "last_login_time"}

	for i, s := range records {
		m = append(m, spanner.InsertOrUpdate("users", accountsColumns, s))

		// Avoid 20k mutation limit
		if i%200 == 0 {
			_, err = client.Apply(ctx, m)
			m = nil
		}
	}

	_, err = client.Apply(ctx, m)
	return err
}

func main() {
	ctx := context.Background()

	// This database must exist.
	databaseName := "projects/<PROJECT_ID>/instances/<INSTANCE_ID>/databases/<DATABASE_ID>"

	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		log.Fatalf("Failed to create client %v", err)
	}
	defer client.Close()

	for i := 1; i <= 3; i++ {
		start := time.Now()
		deleteUsingPartitionedDML(os.Stdout, databaseName, client)
		deleteTime := time.Since(start)
		start = time.Now()
		insertByMutation(os.Stdout, databaseName, client)
		elapsed := time.Since(start)
		fmt.Fprintf(os.Stdout, "%s, %s \n", deleteTime, elapsed)
	}
}
