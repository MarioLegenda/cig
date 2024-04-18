package main

import "fmt"

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
	sql := `              			


SELECT 
    
    
    
    'e.Year'
     ,    'e.Industry_aggregation_NZSIOC' 
		FROM 
		    path:../testdata/example.csv   			AS			     e 
		WHERE 				'e.Year'::int 
		    
		    
		    > 			
		    
		    
		                       '2013'			 
		ORDER 
		    
		    
		    
		    BY 
		    
		    
		    
	'e.Year'        ,
		    
		    
		    'e.Industry_ag      gregation_NZSIOC'                        
		LIMIT                 		10 
		    OFFSET 
		    
		    
		    
		    
		    4
		
		
		
		
		`

	tokens := validateAndParse(sql)

	for _, t := range tokens {
		fmt.Println(t)
	}
}

func validateAndParse(sql string) []string {
	i := 0

	tokens := make([]string, 0)
	buf := make([]byte, 0)
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
			} else if b != 10 && b != 9 && b != 32 {
				buf = append(buf, b)
				i++
				continue
			}

			if len(buf) != 0 {
				tokens = append(tokens, string(buf))
				buf = make([]byte, 0)
				break
			}
		}

		return append(tokens, validateAndParse(sql[i:])...)
	}

	return tokens
}
