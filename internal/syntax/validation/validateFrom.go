package validation

import (
	"github.com/MarioLegenda/cig/pkg"
	"strings"
)

func validateFrom(token string) error {
	if strings.ToLower(token) != "from" {
		return pkg.InvalidFromToken
	}

	return nil
}
