package main

import (
	"fmt"
	"github.com/MarioLegenda/cig"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"os"
	"strings"
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
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
