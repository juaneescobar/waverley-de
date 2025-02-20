# dbt_waverley

A project template for building, testing, scheduling, and running an ELT data
pipeline as a SQL workflow using:

- [dbt-core](https://github.com/dbt-labs/dbt-core)
- [Github Actions](https://docs.github.com/en/actions) used as runner, scheduler, and orchestrator.
- [Python virtual environment](https://docs.python.org/3/library/venv.html).
- [Snowflake](https://docs.snowflake.com/) as a data warehouse option.
- [BigQuery](https://cloud.google.com/bigquery/docs) as a data warehouse option.
- [sqlfluff](https://sqlfluff.com/) used for linting SQL files
- [sqlfmt](https://sqlfmt.com/) used for formatting SQL files

## Getting started

Run `bash setup.sh` once to install project dependencies and configure the desired data warehouse and agent connection.

Run the following target commands to execute the desired SQL workflow operation:
- `dbt compile`
- `dbt test`
- `dbt run`
- `dbt docs generate`
- `dbt docs serve`

## Contributing

`git checkout main`

The `main` branch is read-only.

`git checkout -b <feature-name>`

Raise a PR

Merge PR to main branch


## Code Style Guide

- Follow Mozilla Data Documentation's (SQL Style Guide)[https://docs.telemetry.mozilla.org/concepts/sql_style#sql-style-guide] 
- See dbt docs on [Best Practices](https://docs.getdbt.com/best-practices)

## Default Configuration

By default, everything gets created as a view. You can override that at the directory level so everything in that directory will materialize to a different materialization.


## Resources

- See [repository notes on dbt](./docs/dbt.md)
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
