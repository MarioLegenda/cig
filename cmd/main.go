package main

import (
	"fmt"
	"github.com/MarioLegenda/cig"
)

func main() {
	tryCig()
	/*	f, err := os.Open("../testdata/example.csv")
		if err != nil {
			log.Fatal(err)
		}

		lineReader1 := NewLineReader(f, false)
		fmt.Println(lineReader1())
		fmt.Println(lineReader1())
		fmt.Println(lineReader1())
		fmt.Println(lineReader1())
		fmt.Println(lineReader1())*/
}

func tryCig() {
	/*	sql := `


		SELECT



		    'e.Year'
		     ,    'e.Industry_aggregation_NZSIOC'
				FROM
				    path:../testdata/example.csv   			AS			     e
				WHERE 				'e.Year'::int


				    >


				                       '2013'









				`*/

	sql := "SELECT      *      FROM path:../testdata/example.csv As g WHERE 'g.b'::int = '4' Or 'g.b' = 'b' LIMIT 6 OFFSET 12 ORDER BY 'g.Year'    ,'g.Entity'    DESC"

	c := cig.New()

	result := c.Run(sql)

	fmt.Println(result.Errors(), len(result.Errors()))

	//fmt.Println(result.Result())

}

func validateAndParse(sql string) []string {

	tokens := make([]string, 0)
	buf := make([]byte, 0)
	i := 0
	for i < len(sql) {
		b := sql[i]

		if b == 10 || b == 9 || b == 32 {
			i++
			continue
		}

		quoteMode := false
		for i < len(sql) {
			b = sql[i]

			if b == 39 && !quoteMode {
				quoteMode = true
			} else if b == 39 && quoteMode {
				quoteMode = false
			}

			if quoteMode {
				buf = append(buf, b)
				i++
				continue
			}

			if !quoteMode && b == 44 {
				if len(buf) != 0 {
					tokens = append(tokens, string(buf))
				}

				tokens = append(tokens, ",")
				buf = make([]byte, 0)
				i++
				break
			}

			if b != 10 && b != 9 && b != 32 {
				buf = append(buf, b)
				i++
				continue
			}

			if len(buf) != 0 {
				break
			}
		}

		if len(buf) != 0 {
			tokens = append(tokens, string(buf))
		}

		buf = make([]byte, 0)

		return append(tokens, validateAndParse(sql[i:])...)
	}

	return tokens
}
