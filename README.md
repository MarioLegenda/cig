> [!CAUTION]
> This package is still a work in progress. You can try it out
> but the API might change in future versions but not drastically.

With **cig**, you can query a .csv file with sql syntax.

- [Installation](#installation)
- [Usage](#usage)
- [Why this exists](#why-this-exists)
- [Tasks until finished](#development-tasks-until-the-project-is-finished)

**Important considerations:**

1. Columns to return, columns in where conditions, columns in ORDER BY clause
and values must be enclosed in single quotes. For example:

````sql
SELECT 's.ColumnOne', 's.ColumnTwo' 
FROM path:path_to_csv.csv AS s WHERE 's.ColumnThree' = 'value' 
ORDER BY 's.columnFour', 's.ColumnFive' DESC
````
2. Alias is required. Without the `AS s` part of the above query, the query
would not be able to run.

3. Path to a file must be relative to the executing binary or an absolute path.
Consider always giving absolute path for better portability. 

4. This project does not and will not implement the entire SQL syntax. Other than
tasks outlined in the [Tasks section](#development-tasks-until-the-project-is-finished),
nothing else will be developed except making it faster and maintainable.

5. This is not a project that should be used in production. Its only use is for simple
lookups and nothing else. In most situations, it is better to import a csv file into
a database of your choice. This project is intended as "something interesting to do" for
me so do not take it too seriously.

6. This package will be concurrency safe. This means that `Run()` method
will be able to be used inside your own concurrency primitives. Although
I will try to make it faster using concurrency for very large files,
that will not affect using the public API in your code. 

# Installation

`go get github.com/MarioLegenda/cig@v0.1.1`

# Usage

Below snippet of sql describes almost all current features of this package:

````sql
SELECT * FROM path:path_to_file.csv AS g WHERE 'g.columnOne' = 'string_value'
AND 'g.columnTwo'::int != '65' OR 'g.columnThree'::float = '56.3'
OFFSET 34
LIMIT 56
ORDER BY 'g.columnFour', 'g.columnFive' DESC
````

Instead of `*`, you can specify the columns to return like this:

````sql
SELECT 'g.columnOne', 'g.columnTwo' /** rest of query goes here */
````

If you don't specify `DESC` or `ASC`, `ASC` is assumed. 

In code, you use it like this:

````go
package main

import (
	"fmt"
	"github.com/MarioLegenda/cig"
	"log"
)

func main() {
	c := cig.New()

	result := c.Run(`
SELECT * FROM path:path_to_file.csv AS g WHERE 'g.columnOne' = 'string_value'
AND 'g.columnTwo'::int != '65' OR 'g.columnThree'::float = '56.3'
OFFSET 34
LIMIT 56
ORDER BY 'g.columnFour', 'g.columnFive' DESC
`)

	if result.Error != nil {
		log.Fatalln(result.Error)
	}

	fmt.Println(result.SelectedColumns)
	fmt.Println(result.AllColumns)
	fmt.Println(result.Data)
}
````

Signature of the result is

````go
type Data struct {
    SelectedColumns []string
    AllColumns      []string
    Error           error
    Data            []map[string]string
}
````

You can handle errors with the `errors.Is` function if you need fine grained
control of exactly which error happened.

````go
package main

import (
	"errors"
	"fmt"
	"github.com/MarioLegenda/cig"
	cigError "github.com/MarioLegenda/cig/pkg"
	"log"
)

func main() {
	c := cig.New()

	result := c.Run(`
SELECT * FROM path:path_to_file.csv AS g WHERE 'g.columnOne' = 'string_value'
AND 'g.columnTwo'::int != '65' OR 'g.columnThree'::float = '56.3'
OFFSET 34
LIMIT 56
ORDER BY 'g.columnFour', 'g.columnFive' DESC
`)

	if errors.Is(result.Error, cigError.InvalidAlias) {
		log.Fatalln(result.Error)
	}

	fmt.Println(result.SelectedColumns)
	fmt.Println(result.AllColumns)
	fmt.Println(result.Data)
}
````

This is the full list of errors you can use:

````go

var InvalidToken = errors.New("Expected WHERE or LIMIT, OFFSET, ORDER BY, got something else.")
var InvalidSelectToken = errors.New("Expected 'select', got something else.")
var InvalidSelectableColumns = errors.New("Expected selectable column")
var InvalidDuplicatedColumn = errors.New("Duplicated selectable column")
var InvalidFromToken = errors.New("Expected 'FROM', got something else.")
var InvalidFilePathToken = errors.New("Expected 'path:path_to_file' but did not get the path part")
var InvalidAsToken = errors.New("Expected 'as', got something else.")
var InvalidAlias = errors.New("Invalid alias.")
var InvalidColumnAlias = errors.New("Column alias not recognized.")
var InvalidWhereClause = errors.New("Expected WHERE clause, got something else.")
var InvalidConditionColumn = errors.New("Expected condition column.")
var InvalidComparisonOperator = errors.New("Invalid comparison operator")
var InvalidLogicalOperator = errors.New("Invalid logical operator")
var InvalidValueToken = errors.New("Invalid value token.")
var InvalidDataType = errors.New("Invalid data type.")
var InvalidConditionAlias = errors.New("Invalid condition alias.")
var InvalidOrderBy = errors.New("Invalid ORDER BY")

````

# Why this exists

One use could be in an environment where it is not possible to install a database
just to lookup some values in a .csv file. This package will provide a command line
utility to do so. Other than that, it would be better to import a .csv file into
a database of your choice and use it like that. 

# Development tasks until the project is finished

- [x] Implement logical operators
- [x] Implement all comparison operators (now, only equality works)
- [x] Implement picking columns to return
- [x] Implement OFFSET and LIMIT to implement pagination
- [x] Implement sorting
- [ ] Create a command line utility to use it on the command line
- [ ] Implement JOIN with multiple files
- [ ] Implement options (cache, timeout with context, extremely simple optional indexing on first query execution)
- [ ] Implement splitting work into multiple goroutines
- [ ] Implement solutions from one billion rows challenge
