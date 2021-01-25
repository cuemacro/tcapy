# tcapy change log

## Coding log

* 25 Jan 2021
  * Updated manifest
  * Started adding QuestDB support (not complete)
* 22 Jan 2021
  * Upgraded Pandas to 1.0.5, no longer recommended to use earlier Pandas versions
  * Updated various other libraries in conda environments
* 16 Dec 2020
    * Fixed typo in notes, web server is on port 9500
* 02 Nov 2020
    * Adding init module for testing
* 23 Sep 2020
    * Minor changes to install instructions
* 15 Sep 2020
    * Synching various Python library versions across Docker, Windows and Linux for tcapy
* 14 Sep 2020
    * Fixed findatapy version dependency
    * Removed pymssql dependency (package deprecated) and pytables/tables (now optional)
    * Fixed Jupyter Binder build (fixed version at 1.2.0)
* 13 Sep 2020
    * Removed multiprocessing_on_dill dependency
* 10 Sep 2020
    * Adding support to use Eikon as a FX tick data source
* 26 Aug 2020
    * Updating conda/pip files to Python 3.7 (note: not yet compatible with Pandas 1.x)
* 18 Aug 2020
    * Fixed problem with Docker and nginx port 9500
    * Fixed configuration issue with Redis and Docker
* 14 Aug 2020
    * Starting to add ClickHouse support (not finished yet)
    * Refactored variables in DatabasePopulator
    * Added NCFX parameters in AccessControl
    * DataDumper now takes AccessControl for username/password for databases
    * Changed default web GUI port to 9500 (so no clash with ClickHouse default port)
    * Uses Arrow as default instead of Msgpack (which is deprecated in Pandas) for storing data in Redis
    * Replace Plotly Orca (deprecated) with Plotly kaleido for generating PNG from Plotly
* 03 Aug 2020
    * Adding link for YouTube demo
    * Added dash bootstrap components as a dependency (haven't yet created GUI with them)
* 28 Jul 2020
    * Updated conda environments with newer libraries
    * Improved Excel support
* 27 Jul 2020
    * Added support for Plotly static image plot + write PDF reports in Docker
    * Refactored upload trades and market tick data to a new class DataDumper
    * Added more support for environmental variables
    * Added extra documentation for installation using Docker
    * Refactored tests to reduce duplicated code
    * Bug fix for SQLAlchemy table wrapper
* 23 Jul 2020
    * Added support for MongoDB Atlas instances (can specify connection string directly now)
* 22 Jul 2020
    * Fixed problem collecting trade CSV from tcapyboard GUI and refactored layout
    * Removed use of many deprecated Pandas features like `.ix`
* 21 Jul 2020
    * Remove stress test by default because of out-of-memory messages on GitHub CI
    * Tests pass locally on Docker (still not yet on GitHub CI)
* 20 Jul 2020
    * Docker tests now pass, fixed docker-compose.test.yml
    * Removed logger.warn in project which is deprecated
    * Added extra documentation around Docker installation of tcapy
    * Many other small fixes
* 17 Jul 2020
    * Now added more services to Docker Compose, so tcapy can be fully deployed with Docker
        * Includes web GUI (Nginx + Gunicorn), Celery, databases (MongoDB + MySQL), caching (Redis + MemCached) etc.
    * Added way to add environment variables easily to `constants.py` (also to allow Docker specific variables)
        * No longer have to create our own `constantscred.py` file to store usernames/passwords etc.
* 03 Jul 2020
    * Fixed close connection issue with SQL and MongoDB
    * Updated microstructure notebook
* 01 Jul 2020
    * Fixed database host issue
* 28 Jun 2020
    * Note, issues with Docker with credential error [fix](https://github.com/docker/compose/issues/6517)
        * `~/.docker/config.json` - `credsStore` key may need to be removed with Docker Desktop
    * Fixed connection URL for Microsoft SQL Server
    * Added Redis to Docker compose
* 17 Jun 2020
    * Bug fixes for loading GUI
    * Fix for ResultsSummary aggregation of results
* 16 Jun 2020
    * Adding tcapy quick guide & microstructure notebooks
* 14 Jun 2020
    * Minor changes to Docker
* 13 Jun 2020
    * Beginning process of Dockerizing the project
    * Adding Binder
    * Making tests self contained and reorganized
        * Add necessary test market/trade data to databases before running tests
    * Numerous bug fixes
* 12 Jun 2020
    * Added parameter to adjust market impact by side of trade
    * ResultsForm aggregations now work purely for market data (in addition to trade/order data from before)
        * Can aggregate by multiple dimensions (eg. month & time of day)
    * Strip ticker column before storing in Arctic
    * Bug fixes for Benchmark calculations if trade/market data missing
    * Refactored BenchmarkMarket classes so can't have trade/order data as inputs
* 05 Jun 2020
    * Speeded up non-multithreaded version, by removing some CacheHandles
    * Speeded up date split parallelization
    * Added support to do pure market analysis (without trade data), eg. calculate & return market mid only
    * Added real world case study Jupyter notebook using Swedish asset manager trade data
    * Various bug fixes for combining DataFrame output
* 01 Jun 2020
    * Aggregation of metrics by hour of day in London/New York timezone (and by max/min)
    * Fixed tick count calculation on resample script
    * Upgraded Plotly dependencies
    * Added benchmarks for
        * Median price for order (or a window around trade)
        * Best/worst price for order (or a window around trade)
    * Refactored VWAP/TWAP benchmark code
    * For benchmarks, allowed adjustment for window for
        * Time of day
        * Before/after time of day
    * Made market charts more customizable to add user defined benchmarks
    * Added Jupyter notebook for additional benchmarks and metrics
* 06 May 2020
    * Change default Windows path to `e:\cuemacro\tcapy` from `e:\Remote\tcapy` to 
    make it more similar to Linux default
    * Now downloads tick data from New Change FX/NCFX using their updated API `DatabaseSourceNCFX`
    * Spun out some functionality for splitting up download dates from `DatabasePopulator` to `UtilFunc`
    * Added script to resample tick data and dump as Parquet eg. if we want to use tick data outside of tcapy
    * Fixed issue with `DatabasePopulator` ignoring downloading of points around end of periods if on weekend
    * Updated Dash/Plotly Python dependencies - appears to fix problems with non-updates of GUI so far
* 24 April 2020
    * Changed Redis repo, so installs latest version without compilation (given compilation very slow)
    * Added scripts for creating conda environment via YAML file (quicker installation)
    * Added links to view Jupyter notebooks by nbviewer
    * Bug fix on Excel implementation, now removes previously drawn charts
* 16 April 2020
    * Added Excel addin/spreadsheet to use tcapy (with xlwings)
    * Adding heatmaps (allowing for multiple metrics/breakdowns)
    * Updated Python dependencies
* 11 April 2020
    * Bug fixes for Influx download/upload (still need to speed up writing)
    * KDB fixes for download/upload (with new qPython version)
* 10 April 2020
    * Bug fixes for MySQL download/upload
    * Bug fixes for saving CacheHandle to VolatileCache
    * Bug fixes for periods without market data (including for cross rates)
    * Improved labelling for various charts with aggregation labels
    * Updated Jupyter notebook to reflect changes (eg. aggregation labels)
    * Added support for ChunkStore from Arctic, resulting in a very large speed improvement when fetching data from MongoDB
    * Added Jupyter notebook for populating database (MySQL, SQLite, Arctic and PyStore)
    * Added experimental support for using PyStore (partitioned Parquet files) to store market tick data
        * Having issues with append
    * Added support for SQLite to store trade/order data
    * Refactored ResultsForm init
    * Now uses Parquet as default binary format on disk
    * Updated dependencies (especially Python libraries)
* 02 April 2020
    * Added HTML versions of Jupyter notebooks
    * Added `DataNorm` as a parameter to `DataRequest`
    * Made chart titles neater for `TCAResults` with Jinja/WeasyPrint PDF creation and refactored report generator
    * Added scatter charts and associated methods/classes
* 27 March 2020
    * Various bug fixes 
        * In particular for `VolatileCache`
    * Updated Jupyter notebook - introducing_tcapy.ipynb
        * Note that Dukascopy downloader is not thread safe
        * Additional examples
    * Added Jupyter notebook - compliance_tca_calculations.ipynb
        * Show how to flag outlier trades and compute notional totals/slippage average per broker
    * Using pd.eval in BenchmarkMarketSpreadToMid to speed up calculation
    * TCAResults can now parse the output from JoinTables
* 26 March 2020
    * Added Windows installation instructions for tcapy
* 24 March 2020
    * Fixed install link on README.md
* 20 March 2020
    * First public upload of tcapy
    
## Bugs/performance issues to be fixed (with date of addition)

* 30 Apr 2020
    * ISSUE: web GUI times out after a while, requiring user to refresh web page
    
* 24 Apr 2020
    * BUG: On the very first run of web GUI (after running restart_tcapy.sh), it doesn't display results (but computation 
    works in backend)
    * FIXED: 06 May 2020 - seems fixed by updating Dash dependencies
* 23 Apr 2020
    * PERFORMANCE: Speed up Dukascopy downloading, to reduce initialization time