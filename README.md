
# Qrl analytics

This is a project that gives insights in the qrl blockchain 

The backend and front end are saperated

- The Backend is based on  django and scrapy. 
- Data is stored in a Postgresql database
- Frontend is based on Vue
- Frontend gets it data with an API call (django rest framework)


## Current flow 

-  Scraper gets data every x min from qrl website 
                  ↧
- Save data in Postgresql (raw tables)
                  ↧
- Scripts getting raw data every x min and calculate 
                  ↧
- Saves data in Postgresql (aggregate tables)
                  ↧
- Quantascan Frontend call to the Aggregate Tables              
                                  

## Need help with

- Backend - Better way to access the blockchain data instead of scraping
- Backend - Better performance database calls 
- Backend - Increase error resistant

- Frontend - improve UX

- General - Idea's to improve insights

## Importance List

First priority is to get all the data from the blockchain and make collection of data resistant to errors

After that 
- improvement in speed (backend)
- improvement of UX (frontend)



## License: MIT
