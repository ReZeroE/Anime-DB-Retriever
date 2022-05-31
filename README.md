# Anime Database Retriever 2.0
Python anime database retriever that fetches the data of every anime from year 1940 to 2022 and stores them into a SQLite database (supported by AnilistPython).

![text](https://i.imgur.com/hgL9yeS.png)


## Requirements
```
AnilistPython==0.1.3
```

## Modifiable Variables
Most variables that could be freely modified resides in `main.__init__()`.
- **MAX_ANIME_ID**: The last anime ID to be retrieved (current set at 200,000). The program will retrieve every anime from ID 0 to this value. 
- **BULK_WRITE_THRESHOLD**: Bulk write threshold to the SQL database file. Increase in this value may result in minor performance increase.
- **RATELIMIT_OFFSET**: AniList's rate limit is 90 requests per minute. The time delay can be set to ~0.75 sec.


Note: Although error handling has been tested for overpassing the rate limit (429) and failed data retrieval (404), anime data failed to be retrieved the first time will not be retrieved again.

