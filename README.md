# Weather and Air Quality Dashboard
## Project Goal
The goal of this project is to create a data pipeline that follows medallion data architecture, ingesting data from three main sources:
- [OpenWeather](https://openweathermap.org/) for weather data
- [Open-Meteo](https://open-meteo.com/) for air quality data
- [National Weather Service](https://www.weather.gov/documentation/services-web-api) for weather alert data

I also scraped data from two websites to get coordinate and population data on VA cities:
- [Maps of World](https://www.mapsofworld.com/usa/states/virginia/lat-long.html) for coordinate data
- [City Population](https://www.citypopulation.de/en/usa/cities/virginia/) for population data

## The Stack
Originally I wanted to create full dashboard, but for now I'm focusing on completing just the data pipeline first. I'll get data warehousing and a dashboard working at a later date. I also planned on adding CI/CD and Terraform, but using them is a bit excessive for a such a simple project.

### Scraping
- Beatiful Soup: web scraping
- Pandas: data manipulation
- Jupyter Notebooks: makes exploring scraping results easier

### Data Pipeline
- Apache Airflow: workflow orchestrator for the entire pipeline
- Docker Compose: containerize both the script and Airflow for deployment
- Amazon EC2: will run the Airflow script so it can work 24/7

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
- Airflow and Docker Compose

### To Do
- Get weather module to post parquets to write to S3