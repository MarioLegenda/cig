package validation

import (
	"github.com/MarioLegenda/cig/pkg"
	"strings"
)

func validSelect(tokens []string) error {
	if strings.ToLower(tokens[0]) != "select" {
		return pkg.InvalidSelectToken
	}

	return nil
}
