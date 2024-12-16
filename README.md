# Predico Data Pipeline
This is a collection of functions and pipelines to download data useful to produce forecast for [Elia Predico platform](https://innovation.eliagroup.eu/en/projects/predico-collaborative-forecasting-platform).


## Datasets sources:
* `target.parquet`: [Elia](https://opendata.elia.be/explore/?q=Wind%20power&disjunctive.theme&disjunctive.dcat.granularity&disjunctive.dcat.accrualperiodicity&disjunctive.keyword&sort=explore.popularity_score)
* `gfs_history.parquet` and `gfs_lastrun.parquet`: [AWS Gfs bucket](https://noaa-gefs-pds.s3.amazonaws.com/index.html)

## Datasets description

### `target.parquet`
* `measured`: measurements in MWh. **!!! This is your target !!!**
* `realtime`: still measurements but coming from realtime dataset. Not definitive.
* `*forecast`: the dataset contains also Elia forecast. **!!! Elia forecasts should not be features of your model !!!**
* `loadfactor`: you can use this value to deduce installed capacity if needed: `capacity=measured/loadfactor`

### `gfs_history.parquet`
* `valid_time`: delivery time.
* `time`: assimilation date. It identifies forecastrun.

  
## Workflow

<br><br>
<img src = "https://raw.githubusercontent.com/clarkmaio/predico_datapipeline/refs/heads/main/assets/predico_datapipeline_workflow.png" style="width:1000px;">


## Roadmap
* ✅ Gfs 0.25° ens mean
* ✅ Ecmwf op 0.25°
* ❌ Elia power demand and PV forecast
* ❌ Ecmwf AI
