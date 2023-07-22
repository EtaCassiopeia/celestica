## Celestica CLI

```shell
cargo run -p celestica-cli -- --help
```

```sql
CREATE EXTERNAL TABLE lance_test (
    id  INT NOT NULL,
    name  VARCHAR NOT NULL,
)
STORED AS LANCE
LOCATION '/tmp/df';
```


CREATE EXTERNAL TABLE lance_test (
id  INT,
name  VARCHAR,
)
STORED AS LANCE
LOCATION '/tmp/df';

insert into lance_test (id, name) values(1, 'Mohsen');