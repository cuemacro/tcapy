# Create folders for database
sudo mkdir -p /data/db_mongodb
sudo chown -R mongodb:mongodb /data/db_mongodb
sudo mkdir -p /data/db_mysql
sudo chown -R mysql:mysql /data/db_mysql
sudo mkdir -p /data/db_mysql
sudo mkdir -p /data/sqlite
sudo mkdir -p /data/pystore

# Create folders for CSV dumps of market data
sudo mkdir -p /data/csv_dump
sudo mkdir -p /data/csv_dump/dukascopy
sudo mkdir -p /data/csv_dump/ncfx
sudo mkdir -p /data/csv_dump/trade_order
sudo mkdir -p /data/csv_dump/temp
sudo mkdir -p /data/csv_dump/temp/large
sudo mkdir -p /data/csv_output
sudo chmod -R a+rw /data/csv_dump
sudo chmod -R a+rw /data/csv_output

# Temporary files
sudo mkdir -p /tmp/csv
sudo mkdir -p /tmp/tcapy

# Create log folder
sudo mkdir -p /home/$USER/cuemacro/tcapy/log