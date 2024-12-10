# Predico Data Pipeline
This is a collection of functions and pipelines to download data useful to produce forecast for [Elia Predico platform](https://innovation.eliagroup.eu/en/projects/predico-collaborative-forecasting-platform).


## Data sources:
* **Actual**: [Elia](https://opendata.elia.be/explore/?q=Wind%20power&disjunctive.theme&disjunctive.dcat.granularity&disjunctive.dcat.accrualperiodicity&disjunctive.keyword&sort=explore.popularity_score)
* **Weather forecasts**: [AWS Gfs bucket](https://noaa-gefs-pds.s3.amazonaws.com/index.html)



## Workflow

```mermaid
architecture-beta
    group pdp(cloud)[Predico data pipeline]

    service gfs_aws(database)[Gfs S3] in pdp
    service elia_db(database)[Elia DB] in pdp
    service gh_actions(server)[Github Actions] in pdp
    service hf_ds(database)[Hugging Face Dataset] in pdp

    gfs_aws:L --> R:gh_actions
    elia_db:R --> L:gh_actions
    gh_actions:B --> T:hf_ds



    group user(cloud)[You]
    service user_server(server)[Your PC] in user
    service predico_platform(database)[Predico platform]

    user_server:B --> T:predico_platform

    hf_ds:R --> L:user_server
```
