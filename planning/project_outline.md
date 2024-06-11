## Finpy Project Visison

- ### Data Collection
    - data i want
        - stock
            - which tickers
            - his price
            - options contracts
            - options pricing of those contracts
            - is, cf, bs
            - divs
            - mkt cap
        - economic
            - which series?
        - index
            - which indecies?
        - forex
            - which forex?
    - how to store the lists of data that i want
    - need to generalize certain functions, but for the intro of this project i need to have a specified list
    - it would nice to have etf holdings breakdown as well but i might add that later so i can get the ML stuff off the ground
- ### Data/Store
    - right now im just gonna do a simple sqlite3 db locally
    - next steps
        - i would love to get the db to run in the cloud (aws/gcp/azure)
        - i think doing a mysql db would be nice too with the time series nature
        - try nosql db to see about that
        - time series specific data?

- ### ML
    - using all the data gathered above and using it to run ML models and see if i can gain insight
    - Types of models
       - XGboost
       - lin reg
       - log reg
       - other teqniques
    - Libs
        - sklearn
        - pytorch
        - tensorflow
        - jax
        - xgboost


- ### General package manangement
    - error handling for empty json returns
    - ensure that the genrealized functions have error handling built on the base error handling
