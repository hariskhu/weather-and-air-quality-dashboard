# Weather and Air Quality Dashboard
## Project Goal
The goal of this project is to create a data pipeline that follows medallion data architecture, ingesting data from three main sources:
- [OpenWeather](https://openweathermap.org/) for weather data
- [OpenMeteo](https://open-meteo.com/) for air quality data
- [National Weather Service](https://www.weather.gov/documentation/services-web-api) for weather alert data

I also scraped data from two websites to get coordinate and population data on VA cities:
- [Maps of World](https://www.mapsofworld.com/usa/states/virginia/lat-long.html) for coordinate data
- [City Population](https://www.citypopulation.de/en/usa/cities/virginia/) for population data

## The Stack
I'm using (or plan on using) a variety of tools as part of this project.

### Scraping
- Beatiful Soup: web scraping
- Pandas: data manipulation
- Jupyter Notebooks: makes exploring scraping results easier

### Data Pipeline
- Apache Airflow: workflow orchestrator for the entire pipeline
- Docker Compose: makes deploying the script faster
- Amazon EC2: will run the Airflow script so it can work 24/7
- Terraform: mostly for the learning experience, but also makes it easier to display how S3 and EC2 are set up

#### Bronze Layer
- Amazon S3: holds parquets from API calls

#### Silver and Gold Layers
- Snowflake (and dbt within Snowflake): pulls bronze parquets in, cleans them for the silver layer, then prepares gold layer tables for Tableau to use

### Dashboard
- Tableau: deployable through Tableau Public (no cloud costs and less repetitive work)

## Progress
### Completed
- Scraping notebook
- Data collecting functions
- IAM users and S3 bucket (not with Terraform yet)
- Data collecting functions are Dockerized (but are just being imported for the Airflow script anyway so it was a little pointless)

### To Do
- Get weather module to post parquets to write to S3 (ignore schema for now, just make sure it works)
- Terraform for S3
- Airflow and Docker Compose
- Terraform for EC2
- CD with GitHub Actions
- Consider paying for OpenWeather to track more cities, rerun the scraping notebook if so
- Set up Snowflake and connect with Airflow, CD means that minimal data will be lost
- Create Tableau dashboard and deploy to Tableau Public