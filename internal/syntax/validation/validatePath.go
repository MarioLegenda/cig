package validation

import (
	"fmt"
	"github.com/MarioLegenda/cig/pkg"
	"os"
	"strings"
)

func validatePath(token string) (string, error) {
	// validate csv file path
	splitPath := strings.Split(token, ":")
	if len(splitPath) != 2 {
		return "", pkg.InvalidFilePathToken
	}

	if splitPath[0] != "path" {
		return "", pkg.InvalidFilePathToken
	}

	// get the actual path part and validate that it exists
	path := splitPath[1]
	stat, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("File path %s does not exist: %w", path, pkg.InvalidFilePathToken)
	}

	// validate that the file is an actual .csv file
	nameSplit := strings.Split(stat.Name(), ".")
	if nameSplit[1] != "csv" {
		return "", fmt.Errorf("File %s is not a csv file or it does not have a csv extension: %w", path, pkg.InvalidFilePathToken)
	}

	return path, nil
}
