> [!CAUTION]
> This package is still in development

# Introduction

With **cig**, you can query a .csv file with sql syntax. It is still in development,
but as time progresses, you would be able to filter data in a csv file with SQL syntax.
For example

````sql
SELECT * FROM path:my_data.csv AS e WHERE e.column = 'value'
````

For now, you can test it only with the above example, or without the **where** clause what
will return all the rows. The return data type will be `map[string]string` 

# Installation

`go get github.com/MarioLegenda/cig`

# Future development tasks (for now)

- [ ] Implement logical operators
- [ ] Implement all comparison operators (now, only equality works)
- [ ] Implement picking columns to return
- [ ] Implement OFFSET and LIMIT to implement pagination
- [ ] Implement sorting
- [ ] Implement options (cache?, timeout?)
- [ ] Implement goroutine worker balancer (if needed)
