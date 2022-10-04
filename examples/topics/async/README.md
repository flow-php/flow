# Flow PHP - Asynchronous processing

In most cases, single process should be more than enough to process even huge amounts thanks to `\Generator`. 
In cases where time matters, each batch of [Rows](/src/core/etl/src/Flow/ETL/Rows.php) can be processed
by a standalone PHP process (worker). 

Examples: 

- [csv to db - Async - AMP](csv_to_db_async_amp.php)
- [csv to db - Async - ReactPHP](csv_to_db_async_react.php)
- [csv to text - Async - ReactPHP](csv_to_text_async_react.php)
- [csv to json - Async - ReactPHP](csv_to_json_async_react.php)
- [csv to db - Sync](csv_to_db_sync.php)

Asynchronous processing is possible thanks to custom implementation of the [Pipeline](/src/core/etl/src/Flow/ETL/Pipeline.php) 
interface.

In order to make adaptation to existing projects easier, Flow provides to socket servers/workers. 

- [Amphp](/src/adapter/etl-adapter-amphp/README.md)
- [Reacphp](/src/adapter/etl-adapter-reactphp/README.md)

