# DB2 Locks
Test project for exploring DB2 locking behavior


This tests many variants to obtain a lock for one or multiple rows of a table in a DB2 database.

The result is that 
```
jdbcTemplate.query(
    "select 0 from example where id = 23 for update with rs use and keep update locks", 
    rs -> {
		rs.next();
		return null;
});
```
obtains a lock on the selected row, while this one doesn't:

```
jdbcTemplate.query(
    "select 0 from example where id = 23 for update with rs use and keep update locks", 
    rs -> {
		return null;
});
```

## Running the tests

You need to accept IBM's license by putting a file named `container-license-acceptance.txt` in the class path with the single line of content: `ibmcom/db2:11.5.0.0a`

[I have created a Stack Overflow question about this.](https://stackoverflow.com/q/61681095/66686)
