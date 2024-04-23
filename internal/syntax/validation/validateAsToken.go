package validation

import (
	"github.com/MarioLegenda/cig/pkg"
	"strings"
)

func validateAsToken(token string) error {
	if strings.ToLower(token) != "as" {
		return pkg.InvalidAsToken
	}

	return nil
}
