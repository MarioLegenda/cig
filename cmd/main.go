package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/MarioLegenda/cig"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

var rootCmd = &cobra.Command{
	Use:   "cig",
	Short: "cig allows you to query CSV file with SQL syntax",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := cig.New()

		data := c.Run(strings.Join(args, ""))
		if data.Error != nil {
			return data.Error
		}

		result := data.Data
		if len(result) == 0 {
			fmt.Println("")
			fmt.Println("Empty result returned. Nothing found.")
			fmt.Println("")
			return nil
		}

		columns := make(table.Row, len(data.SelectedColumns))
		for i, c := range data.SelectedColumns {
			columns[i] = c
		}

		rows := make([]table.Row, 0)
		for _, res := range result {
			values := make(table.Row, len(res))
			i := 0
			for _, column := range data.SelectedColumns {
				val := res[column]
				values[i] = val
				i++
			}

			rows = append(rows, values)
		}

		t := table.NewWriter()
		t.SetPageSize(100)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(columns)
		t.AppendRows(rows)
		t.Render()

		return nil
	},
}

func main() {
	f, err := os.Open("../testdata/statistics.csv")
	if err != nil {
		log.Fatalln(err)
	}

	reader := bufio.NewReader(f)

	now := time.Now()
	for {
		chunk, err := readChunk(reader)

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.Fatalln(err)
		}

		convertToLines(chunk)
	}
	fmt.Println(time.Since(now))
}

func convertToLines(chunks []byte) [][]string {
	lines := make([][]string, 0)

	buf := make([]byte, 0)

	for _, chunk := range chunks {
		if chunk == 10 {
			str := string(buf)
			lines = append(lines, strings.Split(str, ","))
			buf = make([]byte, 0)
			continue
		}

		buf = append(buf, chunk)
	}

	return lines
}

func readChunk(reader io.Reader) ([]byte, error) {
	for {
		buf := make([]byte, 10*1024*1024)
		_, err := reader.Read(buf)

		if err != nil {
			return nil, err
		}

		return buf, nil
	}
}
