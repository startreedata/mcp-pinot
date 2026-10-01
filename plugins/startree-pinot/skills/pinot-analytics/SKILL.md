---
name: pinot-analytics
description: Answer questions from Apache Pinot or StarTree Cloud data with the StarTree Pinot tools. Use when the user asks about metrics, counts, trends or tables in Pinot, or wants to inspect or change a Pinot schema or table config.
---

# Working with Pinot through the StarTree Pinot tools

## Find the data before you query it

1. Call `list_tables` to see what exists. Results are paginated; follow `has_more` with `offset` when the list is long.
2. Pick candidate tables by name, then call `get_schema` for each. A table's schema name is normally the table name without an `_OFFLINE` or `_REALTIME` suffix.
3. Note the time column and the dimensions and metrics you need. Call `get_table_config` when you need indexing or ingestion details, for example to see whether a column has an index before filtering on it.

## Query carefully

- `read_query` runs one read-only `SELECT` or `WITH ... SELECT`. Anything else is rejected, so don't try to write data with it.
- Real-time tables can hold billions of rows. Filter on the time column, aggregate rather than selecting raw rows, and always include a `LIMIT`.
- Results come back a page at a time. Read `has_more` and fetch the next page with `offset` only if the user needs more than the first page.
- Quote identifiers that clash with SQL keywords using double quotes.
- Summarize what the numbers show, and include the SQL you ran so the user can check it.

## When something fails

- For an unknown table or column, re-read the schema and correct the name. Don't retry the same query unchanged.
- For a timeout or connection error, call `test_connection` to see whether the broker or the controller is unreachable, then retry once it passes.
- For a permission error, tell the user which call was refused. Their credentials may be read-only on purpose.

## Changing schemas or table configs

`create_schema`, `update_schema`, `create_table_config`, `update_table_config` and `reload_table_filters` change the cluster.

1. Call the tool with `dry_run=true` first and show the user exactly what would change.
2. Apply it with `dry_run=false` and the preview's `confirmation_token` only after the user explicitly confirms. The token expires and works once, so preview again if the user changes their mind about any detail.
3. Never chain a preview and an apply in one step on your own initiative.
