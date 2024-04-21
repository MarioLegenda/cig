package validation

func isEnclosedInQuote(v string) bool {
	return v[0] == '\'' && v[len(v)-1] == '\''
}
