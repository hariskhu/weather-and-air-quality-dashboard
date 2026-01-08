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
- Docker and Docker Compose: Docker to deploy the application, Docker Compose to run Airflow properly

#### Bronze Layer
- Amazon S3: holds parquets from API calls.

#### Silver and Gold Layers
- Snowflake (and dbt within Snowflake): pulls bronze parquets in, cleans them for the silver layer, then prepares gold layer tables.

### Dashboard
- Tableau: more flexibile visualizations

## Progress
### Completed
- Scraping notebook is done
- Data collecting functions almost complete
- IAM users and S3 bucket are ready
- Data collecting functions are Dockerized
### To Do
- Set up Airflow and Docker Compose
- Finish alerts functions
- Connect script to S3
- Set up Snowflake
- CD with something free?