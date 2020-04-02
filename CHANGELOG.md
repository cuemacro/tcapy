# tcapy change log

## Release Notes

* No official releases yet

## Coding log

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
    * Using pd.eval in BenchmarkSpreadToMid to speed up calculation
    * TCAResults can now parse the output from JoinTables
* 26 March 2020
    * Added Windows installation instructions for tcapy
* 24 March 2020
    * Fixed install link on README.md
* 20 March 2020
    * First public upload of tcapy