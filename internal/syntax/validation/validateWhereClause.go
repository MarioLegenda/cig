package validation

import (
	"github.com/MarioLegenda/cig/pkg"
	"strings"
)

func validateWhereClause(token string) error {
	if token == "" {
		return nil
	}
	if strings.ToLower(token) != "where" {
		return pkg.InvalidWhereClause
	}

	return nil
}
