
You can change things between tables and views by changing a keyword rather than writing the data definition language (DDL) to do this behind the scenes.
[https://docs.getdbt.com/guides/snowflake?step=8#change-the-way-your-model-is-materialized]


TODO: --full-refresh
[https://docs.getdbt.com/guides/snowflake?step=8#change-the-way-your-model-is-materialized]


## Materializations supported

dbt ships with five materializations:
- view
- table
- incremental
- ephemeral
- materialized_view.

Check out the [dbt documentation on materializations](https://docs.getdbt.com/docs/build/materializations) for more information on each of these options.

dbt also supports [creating your own custom materializations](https://docs.getdbt.com/guides/create-new-materializations?step=1), if required, however this is an advanced feature of dbt.


## Model configurations supported

dbt allows us to configure:

`tags` to support easy categorization and graph selection
custom schemas to split your models across multiple schemas
aliases if your view/table name should differ FROM the filename
Snippets of SQL to run at the start or end of a model, known as hooks
Warehouse-specific configurations for performance (e.g. sort and dist keys on Redshift, partitions on BigQuery)

Check out the [dbt docs on model configurations](https://docs.getdbt.com/reference/model-configs) to learn more.


## Deployments


### Deployment operation tools

TODO: See https://docs.getdbt.com/docs/deploy/deployments


### Deployment monitoring tools

TODO: See https://docs.getdbt.com/docs/deploy/deployments#monitor-jobs-and-alerts

- See guide to [Post to Microsoft Teams when a job finishes
](https://docs.getdbt.com/guides/zapier-ms-teams?step=1)


### Other references

- See [Materialize queries with exports](https://docs.getdbt.com/docs/use-dbt-semantic-layer/exports)
- See [Integrate with other orchestration tools](https://docs.getdbt.com/docs/deploy/deployment-tools)
- See dbt docs on [Build your metrics](https://docs.getdbt.com/docs/build/build-metrics-intro)
- See [The Ultimate Guide to dbt on Snowflake: Day 1](https://medium.com/@dipan.saha/dbt-on-snowflake-a-comprehensive-guide-a849e893a2e)

## Notes on DBT Cloud

- See dbt post [Optimize Costs with dbt Cloud's Defer to Production Feature](https://www.getdbt.com/blog/optimize-costs-with-dbt-cloud-defer-to-production-feature)

## Notes for VSCode IDE

- TODO: See extension `innoverio.vscode-dbt-power-user`


## Notes for Snowflake 

- See dbt docs [Snowflake setup](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup#account)
- See Snowflake's [BEST PRACTICES FOR OPTIMIZING YOUR DBT AND SNOWFLAKE DEPLOYMENT](https://www.snowflake.com/wp-content/uploads/2021/10/Best-Practices-for-Optimizing-Your-dbt-and-Snowflake-Deployment.pdf)
- See post [Snowflake Queries: How Much Is Each One Costing You?](https://blog.sundeck.io/snowflake-queries-how-much-is-each-one-costing-you-83f40a61dd90)
- See Select post [Calculating cost per query in Snowflake](https://SELECT.dev/posts/cost-per-query#complete-sql-query)

### Cost Monitoring tools

- See Snowflake docs on [Understanding overall cost](https://docs.snowflake.com/en/user-guide/cost-understanding-overall)
- See [Select](https://SELECT.dev/docs/dbt-snowflake-monitoring)
    - [dbt-snowflake-monitoring](https://github.com/get-SELECT/dbt-snowflake-monitoring)

## Notes for BigQuery

- See [BigQuery setup](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
- See [](https://docs.getdbt.com/guides/bigquery?step=1)
- See [How to track your dbt project’s BigQuery data warehouse costs (a PoC)](https://medium.com/inthepipeline/how-to-track-your-dbt-projects-bigquery-data-warehouse-costs-in-dbt-a-poc-716068b1e2c3)
- See [dbt_cloud_run_cost repo](https://github.com/danthelion/dbt_cloud_run_cost)