package validation

import (
	"fmt"
	"github.com/MarioLegenda/cig/pkg"
)

func validateSelectableColumnAlias(alias string, selectableColumns []SelectableColumn) error {
	if len(selectableColumns) == 1 && selectableColumns[0].Column == "*" {
		return nil
	}

	for _, c := range selectableColumns {
		if c.Alias != alias {
			return fmt.Errorf("Expected alias %s, got %s for column %s: %w", alias, c.Alias, c.Column, pkg.InvalidColumnAlias)
		}
	}

	return nil
}
