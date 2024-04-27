package tokenizer

func Tokenize(sql string) []string {
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

		return append(tokens, Tokenize(sql[i:])...)
	}

	return tokens
}
