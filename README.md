<img src="cuemacro_logo.png?raw=true"/>

# [tcapy](https://github.com/cuemacro/tcapy)

tcapy is a Python library for doing transaction cost analysis (TCA), essentially finding the cost of your trading activity. 
Across the industry many financial firms and corporates trading within financial markets spend a lot of money on TCA, either
by developing in house tools or using external services. It is estimated that the typical buy side equities trading desk 
spends around 225k USD a year on TCA (see [MarketsMedia report](https://www.marketsmedia.com/tca-growth-fueled-regulators-investors/)). 
Many sell side firms and larger buy side firms build and maintain their own TCA libraries, which is very expensive. The cost of TCA 
across the industry is likely to run into many hundreds of millions of dollars or possibly billions of dollars.

Much of the complexity in TCA is due to the need to handle large tick datasets and do calculations on them and is largely a
software engineering problem. This work needs to be repeated in every single implementation. By open sourcing the library 
we hope that the industry will no longer need to keep reinventing the wheel when it comes to TCA. At the same time, 
because all the code is visible to users, tcapy allows you can add your own customized metrics and benchmarks, 
which is where you are likely have very particular IP in financial markets. You get the flexibility of a fully internal
TCA solution for free.

tcapy is one of the first open source libraries for TCA. You can run the library on your own hardware, so your trade/order
data can be kept private. It has been in development since June 2017, originally for a large asset manager and was open 
sourced in March 2020.

We've made tcapy to be vendor independent, hence for example it supports:

* multiple database types for storing
market tick data (including Arctic/MongoDB, KDB and InfluxDB) and for trade/order data (including MySQL, PostgreSQL and 
Microsoft SQL Server).
* Linux (tested on Ubuntu/Red Hat) - also it also works on Windows (need Windows Subsystem for Linux to make all features
accessible)

tcapy has also been written to distribute the computation and make a lot of use of caching. In the future, we are hoping to 
add features to make it easy to use serverless computing features on the cloud. Since you can see all the code, it also
makes the TCA totally transparent. If you are doing TCA for regulatory reasons, it makes sense that the process should
be fully open, rather than a black box. Having an open source library, makes it easier to make changes and fitting it to your 
use case.

# What is TCA and why do traders need it?

At its very simplest TCA can involve working out the spread (slippage) you paid. It is also important to understand metrics such as 
market impact. Trading firms need to do TCA for a variety of reasons:

* regulations such as MiFID II which stipulate that buy side firms need to show best execution
* to reduce the cost of trading, by analysing your LP (liquidity providers), execution styles etc. and hence increase alpha

tcapy has been in development for several years and currently supports FX spot based trade/order data. In the future, we are 
also planning adding other asset classes. It has several FX specific features that include:

* conversion of trade/order data booked in wrong convention eg. USDGBP is converted to GBPUSD
* ability to generate synthetic cross rates from benchmark data for exotic crosses if underlying data doesn't exist

# What are trades and orders?

We assume that an order has a start and end time, and hence a duration. It will have fields associated with it such as the
ticker traded, the execution price, the account, the execution trader, the broker, the algo used etc.
 
Underneath every order, there might be a number of trades and other events. The difference is that we assume these trade
events, are points in time. These trades will tag their respective orders.

tcapy can calculate and fill in fields of an order, such as executed notional, executed price based upon it the underlying 
trade fills. In the literature, the terminology can sometimes differ for the names of orders and trades. tcapy can 
also work purely at a trade level, as well on both trades and orders.

# What should the format of my trade and orders be?

tcapy expects your orders in a specific format with specific field names, which can viewed under the `tests_harness_data` folder:
* `small_test_order_df.csv` for the format of the orders
* `small_test_trade_df.csv` for the format of the trades

# What does tcapy do?

tcapy performs TCA. Essentially, it takes as an input your own trade/order data and market tick data. It will merge the 
market tick data with your trade/order data, to calculate benchmarks (such as the mid or arrival price of a trade/order, 
or TWAP/VWAP during  an order). The market tick data input should be representative of the market. At present we have 
developed adapters to download data from Dukascopy (free) and New Change FX (paid). We'd also like to add adapters to
download different data sources.

Importantly, you can use whatever market tick data source you'd like, if its already stored in your market tick database.
Very often you might wish to switch the data source you use for your benchmark.

Based on the benchmark you choose, it computes various metrics on your trade/order data such as:

* slippage - the difference between your benchmark and the executed price
* market impact - the price move following a trade/order

It can then provide statistics based on the metrics such as

* average
* PDF
* outlier

# How can you supply the trade/order and market data to tcapy?

You can provide your trade/order and market tick data in different ways:

* CSV files for trade/order/market data
* DataFrames for trade/order/market data
* MongoDB/Arctic, KDB or InfluxDB for market data
* MySQL, PostgreSQL or MySQL for trade/order data

Obviously, if you choose to use the database options, you'll have to maintain these databases. If you database of choice is not
supported it is possible to write an adapter for it. 

# How do you use tcapy?

There are four ways to use tcapy, which are:

* Via web GUI, built with Dash
* Programmatically
    * Via command prompt/IDE etc.
    * Via Jupyter notebook to do interactive research studies
* Via Excel spreadsheet via xlwings (see example spreadsheet [here](tcapy/excel/tcapy_xl.xlsm))
* Via RESTful API (very basic functionality at present)

Below we have a montage illustrating some of the ways you can interact with tcapy out-of-the-box!

<img src="img/tcapy_montage.png?raw=true" width="750"/>

# Jupyter notebook to show how to use and call tcapy programatically

Given we can call tcapy programmatically, if you are doing a more customized sort of TCA research project, it is ideal
to use Jupyter notebooks to mix code and results in one place.

Sometimes GitHub might not render the Jupyter notebooks, in which case you can use the nbviewer link. Also see
[here](tcapy_notebooks/jupyter_notebooks_description.md), which lists all the notebooks

* [Introducing tcapy and explaining TCA](tcapy_notebooks/introducing_tcapy.ipynb) - how to use tcapy in more detail 
and gives many examples of how to call it programmatically 
([nbviewer](https://nbviewer.jupyter.org/github/cuemacro/tcapy/blob/master/tcapy_notebooks/introducing_tcapy.ipynb))
* [Compliance and other more involved TCA calculations](tcapy_notebooks/compliance_tca_calculations) - how to do 
TCA calculations for compliance and other more involved use cases
([nbviewer](https://nbviewer.jupyter.org/github/cuemacro/tcapy/blob/master/tcapy_notebooks/compliance_tca_calculations))
* [Populating databases for tcapy](tcapy_notebooks/populating_databases_for_tcapy.ipynb) - how to 
populate your trade/order (MySQL/SQLite/Microsoft SQL Server) and market data databases (Arctic/MongoDB/PyStore)
([nbviewer](https://nbviewer.jupyter.org/github/cuemacro/tcapy/blob/master/tcapy_notebooks/populating_databases_for_tcapy.ipynb))
* [Excel/xlwings with tcapy](tcapy_notebooks/excel_xlwings_with_tcapy.ipynb) - how to run tcapy from Excel
with xlwings, with a demo spreadsheet
([nbviewer](https://nbviewer.jupyter.org/github/cuemacro/tcapy/blob/master/tcapy_notebooks/excel_xlwings_with_tcapy.ipynb))

A few things to note:

* Please bear in mind at present tcapy is currently an alpha project
* tcapy is only partially documented, although we are working on adding more documentation, in particular more Jupyter notebooks
* Uses Apache 2.0 licence [see here](LICENCE)

# How do you install tcapy?

Detailed installation instructions can be found [here](INSTALL.md) 

# What technologies does tcapy use?

tcapy is written in Python and makes extensive use of many Python libraries such as Pandas. It also utilises external 
dependencies such as Redis for caching, and can use numerous databases as mentioned earlier. To distribute and speed up 
computation it uses Celery. Hence if we ask for TCA to be performed on multiple tickers, a different Celery worker will
pick up the task from the message broker (Redis) and will return the final results to the results backend (Memcached). 

There is also extensive caching of trade and market data used internally with Redis, so it doesn't repeatedly hit the 
database, which can be slow particularly if you are using over a network with a lot of latency.

# Contributors

Contributors are always welcome for tcapy. If you'd like to contribute, have a look at
[planned features](PLANNED_FEATURES.md) for areas we're looking for help on. If you have any ideas for improvements
to the libraries please let us know too.

# Screenshots

Below we show screenshots of the web GUI for tcapy.

### Detailed TCA GUI

<img src="img/std_gui.png?raw=true" width="750"/>

### Plot your trade executions/order

<img src="img/std_executions.png?raw=true" width="750"/>

### Plot distribution of slippage by different criteria

<img src="img/std_distribution.png?raw=true" width="750"/>

### Write a TCA request in a Pythonic manner

<img src="img/std_tca_request.png?raw=true" width="750"/>

### Plot timeline of slippage

<img src="img/std_timeline_slippage.png?raw=true" width="750"/>

# Requirements

See [installation guide](INSTALL.md) for details on requirements.

# Installation

For detailed installation instructions for tcapy and its associated Python libraries go to the
[installation guide](INSTALL.md). 

# tcapy examples

In tcapy/tcapy_examples you will find several example scripts to demonstrate how to run tcapy, as well as in the 
[introductory Jupyter notebook](tcapy_notebooks/introducing_tcapy.ipynb) 

# Support and sponsorship for new features

We'll of course try to answer your queries if you raise an issue on GitHub.

If you are interested in getting commercial support for tcapy or would like to sponsor the addition of new features for tcapy, 
please contact saeed@cuemacro.com - we've got lots of ideas for ways to improve tcapy, but need support to implement them.

If you're a financial firm, and are keen to make TCA transparent, and would like to sponsor the project, let us know.

# Release notes and coding log

A change log is available [here](CHANGELOG.md)
