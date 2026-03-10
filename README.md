# mkpipe-loader-mariadb

MariaDB loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into MariaDB tables via JDBC.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  mariadb_target:
    variant: mariadb
    host: localhost
    port: 3306
    database: mydb
    user: myuser
    password: mypassword
```

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_mariadb
    source: pg_source
    destination: mariadb_target
    tables:
      - name: public.orders
        target_name: stg_orders
        replication_method: full
        batchsize: 10000
```

---

## Write Parallelism & Throughput

```yaml
      - name: public.orders
        target_name: stg_orders
        replication_method: full
        batchsize: 10000
        write_partitions: 4
```

- **`batchsize`**: rows per JDBC batch `INSERT`.
- **`write_partitions`**: reduces concurrent JDBC connections via `coalesce(N)`.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table name |
| `target_name` | string | required | MariaDB destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `batchsize` | int | `10000` | Rows per JDBC batch insert |
| `write_partitions` | int | — | Coalesce DataFrame to N partitions before writing |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |