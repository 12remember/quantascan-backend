
# Quantascan - Backend

Quantascan is an analytics website for the quantum resistant ledger blockchain.

This is the backend of quantascan.io

The backend and frontend are separated and hosted on Heroku on two different dyno's

- The Backend is based on Django and Scrapy. 
- Data is stored in a Postgresql database
- Frontend is based on Vue


## Current dataflow 

- Scraper gets data every 10 min from qrl explorer website
                  ↧
- Save data in Postgresql (raw tables)
                  ↧
- Scripts getting raw data every 1 hour and calculate 
                  ↧
- Saves data in Postgresql (aggregate tables)
                  ↧
- Quantascan Frontend call to the Aggregate Tables              
                                  

## Known issues, possible solutions and help

### Missing data -> caused by scraping error
Backend - Better way to access the blockchain data instead of scraping , directly from the node ?
Looking for help to think about a better design and build it

### Slow Queries for calls from front-end and inside analytics script to calculate and store data to aggegration tables > 
Backend - Better performance database calls 
Looking for help to think about a better design and build it
Looking for help to optimize queries

### Heroku changes database credentials every x time(months+) > resulting in failing to store new data
If you know a way to to prevent this, please let me know

### Verify Data and error handling
Backend - get insights in data quality and dealing with errors for a better uptime 


### General - Idea's to improve insights or dataflow
Let me know if you have idea's to improve quantascan.io.



# Getting started

How to run Quantascan - backend Locally

## Create map and Clone github
1. Create a map witht he name "QRL" !important
2. Clone backend and frontend to the "QRL" map
3. Check if map structure is :
___ QRL 
_____ quantascan-backend 
_____ quantascan-frontend


## Create Postgresql database 
1. Follow the steps from [Setting up a local postgresql database](https://www.prisma.io/dataguide/postgresql/setting-up-a-local-postgresql-database"prisma.io")

2.  Run postgresql 12

3. Credential for local use:
    hostname = '127.0.0.1'
    username = 'postgres'
    password = 'postgres' # your password
    database = 'qrl'
    port = '5432'

4.  Start the server
5.  Check if you can access the database with a program like [pgadmin](https://www.pgadmin.org/"pgadmin") 

## Run django
1. Go to map ""/QRL/quantascan-backend"
2. Open terminal
3. run "pipenv shell"
4. run "pipenv install". This will install all required packaging
5. run "python manage.py migrate". This will create all tables in the database
6. run "python manage.py runserver". Will start the backend
6. Access site in the browser , 127.0.0.1:8000


## Fill local database with data
starting the scraper to get some data to the local database 
1. go to map ""/QRL/quantascan-backend/qrl_scraper"
2. Open terminal
3. run "pipenv shell
4. run "pipenv install". This will install all required packaging
5. run "scrapy crawl qrl_network_spider"
6. Spider will run, depending on how much data you want
7. the longer the spider runs the more data 
8. If you want to quit crawling press ctrl + c, the spider will stop



## Run analytics Scripts
to fill the aggegrated tables python scripts are used for the calculaton
1. go to map "/QRL/quantascan-backend/qrl_analytic_scripts"
2. open a terminal and run 
3. python add-wallet-stats.py
4. python analyze-blocks-daily.py
5. python analyze-transactions-daily.py
On the server I've created a cronjob for this


## Note 
Be gentle for the qrl server, as scraping will load their server

If you can't get it to work, please let me know !


## More information

[Quantascan.io](https://www.quantascan.io "Quantascan.io")

[The Quantum resistant Ledger](https://www.theqrl.org/ "The QRL homepage")

[Discord Chat](https://discord.gg/RcR9WzX "Discord Chat") @12remember



## License: MIT
