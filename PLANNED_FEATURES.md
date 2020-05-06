<img src="cuemacro_logo.png?raw=true"/>

# tcapy planned features

Here are some of the features we'd like to add to tcapy which include the below. If you'd be interested in contributing 
or sponsoring the addition of new features, please contact saeed@cuemacro.com

* Adding more metrics and benchmarks
* Adding more asset classes
    * Cash equities, FX swaps etc.
* Add wrappers to enable tcapy users to use external TCA providers with tcapy
    * This would make it easier to compare TCA output between providers and compare against internal computation
* Add feature to also tcapy to consume streaming market data from a realtime datafeed to dump to database
    * For example using Redis Streams or Kakfa
* Adding ability to do more general computations (non-TCA) on market data in the same framework
    * Eg. calculate volatility based on market tick data
* Adding more visualisations from Plotly
* Adding more data providers for market tick data
* Adding more database wrappers both for trade/order data and market data (eg. [PyStore](https://github.com/ranaroussi/pystore))
* Making it easier to install and start tcapy
    * Creating a Docker container for tcapy
    * Improving the installation/starting scripts for tcapy
* Adding more ways to interact with tcapy
    * Eg. RESTful API client and Excel wrapper
* Making it easier to configure for parameters which can change often, such as tickers and storing these in a flat
database like SQLite
* Add authentication for the web app
* Investigating the use of Dask Dataframes for distributed computation and profiling the code more to make it faster
* Adding full support for cloud services like AWS, Google Cloud and Azure to take advantage of serverless computing and
easy setup on the various cloud services (eg. AWS Lambda, Cloud Functions & Azure Functions)
* Adding more unit tests to cover more functionality

