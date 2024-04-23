package validation

import "github.com/MarioLegenda/cig/pkg"

func validateAlias(token string) (string, error) {
	if token == "" {
		return "", pkg.InvalidAlias
	}

	return token, nil
}
