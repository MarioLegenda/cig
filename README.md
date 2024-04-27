> [!CAUTION]
> This package is still a work in progress but can be used to try it out

# Introduction

With **cig**, you can query a .csv file with sql syntax.

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

# Installation

`go get github.com/MarioLegenda/cig`

# Usage

This package follows the SQL standard but on a more simpler level since it does
need to. 

# Future development tasks (for now)

- [x] Implement logical operators
- [x] Implement all comparison operators (now, only equality works)
- [x] Implement picking columns to return
- [x] Implement OFFSET and LIMIT to implement pagination
- [x] Implement sorting
- [ ] Implement JOIN with multiple files
- [ ] Implement options (cache?, timeout?)
- [ ] Implement goroutine worker balancer
