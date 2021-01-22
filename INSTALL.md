<img src="cuemacro_logo.png?raw=true"/>

# tcapy installation on Linux

tcapy has been tested with Linux. tcapy should work on different variants of Linux including:

* Ubuntu (I'd recommend this, as it's is generally easier to install and use)
* Red Hat (note that it may require a subscription to install some dependencies)

Installation scripts have been written to work with either Ubuntu or Red Hat (the main differences are using `apt-get`
vs. `yum` installation managers, as well as differences in the package names on the two variants of Linux.

Note you may need slight editing of `set_tcapy_env_vars.sh` for CentOS/Amazon Linux or Debian, and tcapy hasn't been tested on these platforms). We would recommend using Ubuntu, given that
in general it is easier to use. Furthermore, Ubuntu is also available on WSL (Windows Subsystem for Linux), which we
discuss later.

tcapy is primarily tested for use with Python 3.7 and does not support any version of Python 2, and do not use it with earlier
versions of Python 3. We also recommend Pandas 1.0.5. Versions of Pandas before 1 are not supported.

## Use pip 

If you want to use parts of tcapy as a dependency in your own Python application, you could install it using pip

    pip install git+https://www.github.com/cuemacro/tcapy
   
If you do this you'll still likely need to install all the various dependencies etc.

## Download tcapy to your machine

* The first step is to clone the tcapy project from GitHub, in folder `/home/$USER/cuemacro/tcapy` either using the command
below, or from your web browser
* We assume that the $USER in Linux is `tcapyuser` and for the purposes of the tutorial we shall assume that users have 
installed it in folder `/home/tcapyuser/cuemacro/tcapy`, and we would recommend sticking to this folder, although in 
practice you can install anywhere
* You can install Git using `sudo yum install git` (Red Hat/CentOS) or using `sudo apt-get install git` on Ubuntu
        
        mkdir -p /home/tcapyuser/cuemacro/tcapy
        git clone https://github.com/cuemacro/tcapy.git /home/tcapyuser/cuemacro/tcapy
        
it in other folders if you want
* You can of course use `ln` to create a symbolic link from a source directory to `/home/$USER/cuemacro/tcapy` eg.
        
    ln -s /some/source/dir /home/$USER/cuemacro/tcapy
 
## Editing constants

You will need to edit several files so tcapy knows where it has been installed
* Edit file `/home/tcapyusercuemacro/tcapy/batch_scripts/linux/installation/set_tcapy_env_vars.sh` 
    * change `TCAPY_CUEMACRO` parameter to the installation folder
    * adjust any other parameters (eg. related to web server, what is your `conda` path etc.)
* Edit file `/home/tcapyuser/cuemacro/tcapy/tcapy/conf/mongo.conf` so the logpath parameter points to the log file area under
the tcapy folder eg. `logpath = /home/tcapyuser/cuemacro/tcapy/log/mongo.log`
* Edit file `/home/tcapyuser/cuemacro/tcapy/tcapy/conf/redis.conf` if necessary to change how it deals with ejecting elements
from the cache and also the memory size of the store

Look at `/home/tcapyuser/cuemacro/tcapy/tcapy/conf/constants.py` where there are a large number of parameters defined for the
project. These values include those related to the:

* where the test data folder is
* external data vendors configurations
	* tickers
* parameters for the metrics (like market impact)
* logging format
* database configuration
	* IPs of the database servers
	* table names
* the possible values associated with your trade/order data, including
    * tickers
    * brokers
    * portfolios
    * portfolio managers
    * accounts
    * algos
* parallelisation parameters
    * whether to `use_multithreading` (generally recommended to be set to `True`, except when debugging)
* volatile caching for Redis
* Celery settings
* web server parameters

In `/home/tcapyuser/cuemacro/tcapy/tcapygen/constantsgen.py` are parameters specifically related to the GUI
* Dash callbacks
* which lines to plot on charts for Dash
* colors of lines to be plotted

It is recommended you make an additional file `constantscred.py` in `/home/tcapyuser/cuemacro/tcapy/tcapy/conf/`, such as 
below, where you can set any parameters you want to override from `constants.py` and `constantsgen.py`, in particular 
* usernames and passwords for databases you wish to use for market/trade/order data
* IPs for databases
* the properties associated with your trade/order data, like the tickers who trade, brokers you use etc.

Note that `constantscred.py` should not be added to version control to avoid sensitive data on usernames/passwords being
stored there. Whenever you clone a new version of tcapy, make sure to keep a backup of `constantscred.py` to copy back
into `/home/tcapyuser/cuemacro/tcapy/tcapy/conf/`.

Below, we've put a heavily simplified example of `constantscred.py` (obviously make sure your passwords are stronger
than these!). Alternatively, you can set a lot of these as environment variables in the `constants.py` file, which 
tcapy will pick up later and this is how it is typically used by Docker.

```
class ConstantsCred(object):
    
    arctic_username = 'colonelsanders'
    arctic_password = 'megabucket'

    ms_sql_server_username = 'colonelsanders'
    ms_sql_server_password = "megabucket" 
    
```

## Docker Installation of tcapy on Linux

Usually, when we run code, we need to install all the dependencies in our OS. This can be cumbersome, because our
various apps may have conflicting dependencies. An alternative is that we run every app in its own sandbox, a separate 
virtual machine with it's own operating system. The downside of this is that we are going to use lots more resources. 
Docker containers enable us to run applications independently, with their own dependencies. However, the major plus is that
we don't need to spin up a new OS for each container, thus making them more efficient compared to VM.

I've made a Docker container to install tcapy (and will also to have tcapy available in `pip`) and all its various
dependencies. We'll assume that the Linux user is `tcapyuser`.

Thanks to Thomas Schmelzer (@tschm) for working on this element 
and helping me a lot on the Docker part.

* First you need to have installed Ubuntu/WSL2 or have access to a Ubuntu box (eg. on AWS)
* Clone the tcapy project as described above from GitHub onto your local machine in a folder like `/home/tcapyuser/cuemacro/tcapy/`
* Install Docker
    * On Ubuntu/WSL2 download (Docker Desktop for Windows)[https://docs.docker.com/docker-for-windows/install/] 
    * On Ubuntu (without WSL2) see (Docker's official instructions)[https://docs.docker.com/engine/install/ubuntu/]
* On Ubuntu/WSL2, you may have issues with Docker's Desktop for Windows path
    * `rm ~/.docker/config.json` usually fixes the problem
    * See https://github.com/docker/compose/issues/7495 for other fixes and an explanation

We need to create many folders on our host machine, for Docker and non-Docker versions, to store data. 
These will be linked to our containers. This will allow
us to persist data more easily (rather than having them hidden in the container). We have deliberately chosen different 
folders to the standard ones for MongoDB and MySQL. We want to avoid the situation where these 
folders are shared between our containers and databases in the host machine. Also make sure to create a `log` folder, under `/home/tcapyuser/cuemacro/tcapy` 
and various temporary folders, that are specified in the `constants.py` file. The script `create_tcapy_working_folder.sh` can be
run to create these.

    ./home/$USER/cuemacro/tcapy/batch_scripts/linux/installation/create_tcapy_working_folders.sh

In `/home/tcapyuser/cuemacro/tcapy` create a `.tcapy.env` file that has environment variables to be used by the various
Docker containers. In particular this will be useful for holding the usernames and passwords. 

If it's an empty file, the defaults from `constants.py` will be used instead, which won't be as secure. To create an empty
`.tcapy.env` file, simply run `touch .tcapy.env` in the folder `/home/tcapuser/cuemacro/tcapy`. Note, you need to make
sure you have a `.tcapy.env` file even if it is empty!
 
Here is a sample `.tcapy.env` file below:

    MYSQL_USER=root
    MYSQL_PASSWORD=blah_blah_
    MYSQL_ROOT_PASSWORD=blah_blah_
    MYSQL_DATABASE=trade_database
    MONGO_INITDB_ROOT_USERNAME=admin_root
    MONGO_INITDB_ROOT_PASSWORD=blah_blah_
 
You can also add additional fields here, if you edit `constants.py` to pick up these environment variables. When using
Docker `constantscred.py` is ignored. This allows you to have a different configuration when running via Docker vs.
running directly. Also, your username/passwords won't be put in the container this way, by using a `.env` file.
    
If you are using an external instance of MongoDB (eg. MongoDB Atlas), set the connection string, so this instance is used
instead of MongoDB spawned by Docker (you should also edit docker-compose.yml so the mongo service no longer starts): 

    MONGO_CONNECTION_STRING=mongodb+srv://<username>:<password>@cluster0.blah-blah.mongodb.net/?retryWrites=true&w=majority
    
In practice, you may wish to use different users other than `root` for your databases, when you configure them to 
minimize access.

`docker-compose.yml` defines all the various containers which will need to be installed. For some of these images 
they'll also need to run the `Dockerfile`, this will for example setup the Python environment, and copy the various 
tcapy files to the container. At present the Docker version doesn't currently include the dependencies necessary 
for creating PDF reports. Below, we list the containers created by `docker-compose.yml`, and we've also setup
the appropriate Python environment for those containers running tcapy's Python code:

* nginx - web server sitting front of gunicorn
* gunicorn_tcapy - WSGI application server for main tcapy web GUI
* gunicorn_tcapyboard - WSGI application server for trade CSV drag & drop tcapy web GUI
* jupyter - Jupyter notebook server
* celery - distributed task manager
* redis - in-memory key/value store for caching and Celery message broker
* memcached - in-memory key/value store for Celery results back end
* mongo - NoSQL database for storing market tick data
* mysql - SQL database for storing trade/order data

In the folder `/home/tcapyuser/cuemacro/tcapy/` run the following commands:

* `docker-compose build` - this will build all the various tcapy services, it might take a while, because it will 
involve a lot of downloading of all the dependencies and collecting them.
* `docker-compose up` - this can be used to run all the containers

We can then get access to many of the services from our host machine:

* `http://localhost:9500/tcapy` - main tcapy web gui
* `http://localhost:9500/tcapyboard/` - tcapy web gui for uploading trade CSVs
* `http://localhost:8888` - Jupyter notebook with tcapy
* `localhost:6379` - Redis
* `localhost:3306` - MySQL
* `localhost:27017` - MongoDB
* `localhost:11211` - MemCached

Note, that to get full benefit of tcapy, we'll need to populate the market data (mongo) and trade/order data (mysql)
databases, which you'll likely do via your own host machine. Alternatively, if these already exist and are 
running elsewhere, you'll need to make tcapy point to them and also remove these dependencies from the 
`docker-compose.yml` file.

You can also for example use the databases on your host OS, if these are already setup, but again you'll need to change
the IP address to `host.docker.internal` which Docker will recognise within the container as being the host IP.

### Useful commands for managing Docker containers and images

Docker is a very useful tool. In order to use it, it's worth knowing a few simple Docker commands
to help you manage your Docker images and containers, which we list below:

* `docker images` 
    * list all Docker images on disk
* `docker ps`
    * see all the containers running
* `docker kill $(docker ps -q)`
    * kill all running Docker containers
* `docker exec -it <container name> /bin/bash`
    * to get shell access into a Docker container
    * can be useful for troubleshooting
    * if we want to run a Python script, we could run it in the Jupyter container
* `docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' <container name>` - 
    * get the IP of a container
* `docker image rm -f <service>` 
    * forced removal of image
* `docker rmi -f <service>` 
    * forced removal of image
* `docker system prune -a` 
    * delete all containers
    * warning, it will take a long time to build after this!

Here are some commands focused on `docker-compose` options

* `docker-compose rm -v mongo` 
    * remove of anonymous volume of a service
    * this can be necessary with databases if you're trying to change the password, switch to a host OS volume etc.
* `docker-compose build --force-recreate` 
    * builds but forces the recreation of images, rather than using cached versions
* `docker-compose down` 
    * spin down the services

You might also try to login manually into your various containers from your host OS, for `mysql` or `mongo`:
* `mysql -h localhost -P 3306 --protocol=tcp -u root` to login into the MySQL container
* `mongo localhost:27017 -u tcapyuser -p yourpassword`

You might end up with problems if you are running the same databases/processes on your host OS and also in the containers
as they will likely try to grab the same ports. 

Running `/home/tcapyuser/cuemacro/tcapy/batch_scripts/linux/kill_tcapy.sh` before you kick off your Docker containers
will help to kill any dependencies which might be running on your host OS, which could these conflicts such as MySQL
or MongoDB.

### Testing tcapy Docker containers

Just as `docker-compose.yml` defines all the various containers for the production tcapy instance,
`docker-compose.test.yml` defines the containers required to test tcapy. These containers include `mongo`, `mysql` 
and `celery` to test the various database and distributed computation functionality of tcapy. The `sut` 
service is the main test container. Note, when testing 

In the folder `/home/tcapyuser/cuemacro/tcapy/` we can run tests on tcapy via containers by running:
    
    make test

This is the equivalent of running `docker-compose -f docker-compose.test.yml run sut` and that
will run all the tcapy tests. Note, that the test databases are deliberately not mapped to directories the host OS, to avoid
any conflict between the production and test runs of tcapy. If you switch between production `docker-compose.yml`
and `docker-compose.test.yml` you may need to delete the anonymous volumes of the databases. To build
the test run `docker-compose -f docker-compose.test.yml build`

## Non-Docker Installation of tcapy on Linux

If you do not use Docker, you'll need to install all tcapy dependencies directly on your host OS Linux instance. 
tcapy has many dependencies, which need to be installed after cloning the tcapy project locally. We discuss what you should install below.

* Anaconda Python - It is recommended you install the Anaconda distribution of Python first, if you don't already have it. 
This includes `conda` installation manager, which tends to be easier to use when installing certain libraries, which are more
difficult to install using `pip`. First change directory by running 
`cd /home/tcapyuser/cuemacro/tcapy/batch_scripts/linux/installation` and then run `./install_anaconda.sh` 

* Check if `conda` command is accessible from your Bash shell (usually after Anaconda installation, you'll have to reopen
the Bash shell to test this) - and also make sure you have changed the `CONDA_ACTIVATE` parameter in `set_tcapy_env_vars.sh`
to wherever your Anaconda is installed (below shows the default path)
    * `CONDA_ACTIVATE` by default is `/home/tcapyuser/anaconda3/bin/activate`
    
Once you have installed Anaconda, we first change directory by running `cd /home/tcapyuser/cuemacro/tcapy/batch_scripts/linux/installation`
and then run `./install_all_tcapy.sh` which will install a number of dependencies (some of which are optional, so you 
can choose to skip). We paste the code below, with comments.

    # Install Python setup tools, gcc (compiler) and Apache web server etc.
    source install_python_tools_apache.sh
    
    # Setup the virtual Python environmnent (py37tca) - by default conda environment from environment_linux_py36.yml
    source install_virtual_env.sh
    
    # Install the Microsoft SQL Server driver on Linux (only necessary if we want to use SQL Server for trade data)
    # assumes that Microsoft SQL Server has already been installed (or you are accessing it over a network)
    sudo ./install_sql_driver.sh
    
    # Install all the Python packages in the py37tca environment
    # If the conda environment has not already been created from the environment_linux_py37tca.yml file (default)
    # It is generally quicker to create from YML file rather than running conda/pip for each library
    source install_pip_python_packages.sh
    
    # Install nginx web server (primary web server supported by tcapy)
    source install_nginx.sh
    source install_tcapy_on_nginx_gunicorn.sh
    
    # Install database for tick data (MongoDB)
    # note that we can run MongoDB, MySQL and Redis on different computers
    source install_mongo.sh
    
    # Increases the number of open files for root user (for MongoDB)
    source increase_file_limits.sh
    
    # Install database for trade/order data (MySQL) - PostgreSQL also supported
    source install_mysql.sh
    
    # Install Memcached as a results backend for Celery (recommend on the same server)
    source install_memcached.sh
    
    # Install RabbitMQ as a results backend for Celery (AMPQ as a message broker is deprecated in Celery)
    # source install_rabbitmq.sh
    
    # Setup the tcapy application so that it can be picked up nginx/gunicorn
    source install_tcapy_on_nginx_gunicorn.sh
    # source install_tcapy_on_apache_gunicorn.sh
    # source install_tcapy_on_apache.sh # uses WSGI, but this tends to be slower
    
    # Install wkhtmltopdf for converting HTML to PDF
    source install_pdf.sh
    
    # Install weasyprint dependencies
    source install_weasyprint.sh
    
    # Install Jupyter extensions
    source install_jupyter_extensions.sh
    
    # Install Redis key-value store for general caching and as Celery message broker (recommend on same server)
    source install_redis.sh
    
    # We need to open ports to allow access to MongoDB and to give web access to specific clients
    # source add_ip_to_firewall.sh

For the databases, you will need to make sure they are populated with trade data and also market data. tcapy includes
various Python scripts for populating a market tick database from external sources (Dukascopy and NCFX at present), and
from CSV files. There are also scripts for populating the trade/orders database from CSV files. Typically, many 
organisations are likely to already have market tick and trade/orders databases, which are already installed and
maintained.

As you can see the Docker installation is somewhat quicker.

From a firewall viewpoint, it is recommended to prevent Celery from being accessed by other machines 
(which may send malicious pickled objects). Also make sure that Redis and Memcached are not accessible from other machines 
as well. Alternatively, restrict their access to only the IPs which are absolutely necessary. It is likely the majority
of issues in a corporate environment will be due to firewall issues.

# tcapy installation on Windows

If you only have a Windows machine, you have several options, when it comes to installing tcapy:

1. install Linux on Windows in a virtual machine using [VirtualBox](https://www.virtualbox.org/) (or similar applications) 
and then install tcapy on VirtualBox/Linux
    * instructions are [here](https://itsfoss.com/install-linux-in-virtualbox/) for installing Ubuntu in VirtualBox
    * then follow the instructions earlier ie. *tcapy installation on Linux*
        * you might need to enable shared folders (so you can read Windows folders in Linux)
        and enable your permissions to read these `sudo usermod -G vboxsf -a tcapyuser`
        * to create a link run `ln -s /some/source/dir /home/tcapyuser/cuemacro/tcapy`
        * you can SSH into your VirtualBox Ubuntu using a tool like Putty
            * to do this you can enable the VirtualBox Host Adapter
            * install SSH on Ubuntu by running `sudo apt-get install openssh-server`
            
2. install Linux using Microsoft's own Windows subsystem for Linux (WSL) and then install tcapy on WSL/Linux
    * WSL is a compatibility layer for running Linux binary executables natively on Windows 10
    * makes it easier to run Linux under Windows compared to using a virtual machine application such as VirtualBox
    * some of the Linux distributions that can be installed relatively easily on WSL include Ubuntu
    * instructions are [here](https://docs.microsoft.com/en-us/windows/wsl/install-win10) for WSL1
    * instructions are [here](https://docs.microsoft.com/en-us/windows/wsl/wsl2-install) for WSL2
        * WSL2 is now officially released, and I now use that
        [generally available shortly in the full  Windows 10 version 2004](https://devblogs.microsoft.com/commandline/wsl2-will-be-generally-available-in-windows-10-version-2004/)
        * WSL2 offers better compatibility with Linux and offers much faster IO (under certain circumstances
        [Compare WSL1 vs WSL2](https://docs.microsoft.com/en-us/windows/wsl/compare-versions))
    * then follow the instructions earlier ie. *tcapy installation on Linux*
    * some dependencies may work, but they are not officially supported on WSL 
        * eg. MongoDB on WSL, although in this instance, there is a Windows version of MongoDB you could use
    * also WSL doesn't support all Linux functionality, although this will likely change in newer versions
    * it is easy to access files in WSL in Windows and vice versa
        * view WSL/Linux files on Windows by navigating to `\\wsl$`
        * view Windows files in WSL/Linux by navigating to `/mnt/c` (for example for C drive)

3. install tcapy directly on Windows, but some libraries may not be fully supported (eg. Celery)
    * we assume that you've already installed any databases you'd like to use (eg. MongoDB for market tick data, 
    Microsoft SQL Server for your trade/order data)
    * we strongly recommend that you install Ubuntu on WSL (with tcapy) before you install tcapy on Windows
        * but if you aren't going to install WSL, you might also find it useful to install Redis for Windows, 
    and you can download a compiled [old version Redis 3.2 from Microsoft's archive on GitHub](https://github.com/microsoftarchive/redis/releases) 
    but note that this is not officially supported by Redis, who recommend running Redis on Windows
    * download [Anaconda Python distribution for Windows](https://www.anaconda.com/distribution/) and then install in folder
    `C:\Anaconda3`
    * it is possible to use other distributions of Python, but the project has been setup by default and tested to use
    Anaconda Python and conda installation manager
    * as with all the other cases, we need to clone the tcapy project from GitHub either via Git or manually
    * install [Git for Windows](https://gitforwindows.org/) and then run (change local path of tcapy as appropriate)
    
            git clone https://github.com/cuemacro/tcapy.git e:\cuemacro\tcapy
    
    * this will clone it in the `e:\cuemacro\tcapy` folder (you can choose to install it elsewhere), alternatively, you
    can manually clone it from the GitHub website
    * edit `e:\cuemacro\tcapy\batch_scripts\windows\installation\set_tcapy_env_vars.bat` if necessary change the several variables
        * `TCAPY_CUEMACRO` - with the folder you installed tcapy (default: `e:\cuemacro\tcapy`)
        * `CONDA_ACTIVATE` - the path to Anaconda conda (default: `C:\Anaconda3\Scripts\activate.bat`)
    * run `e:\cuemacro\tcapy\batch_scripts\windows\installation\install_virtual_env.bat` which will setup a new 
    conda environment called `py37tca`
    * run `e:\cuemacro\tcapy\batch_scripts\windows\installation\install_pip_python_packages.bat` which will install
    all the packages you need in the `py37tca` environment for the tcapy library
    * you can optionally also run `e:\cuemacro\tcapy\batch_scripts\windows\installation\install_jupyter_extensions.bat` if 
    you're planning to use tcapy from Jupyter, and it will add some useful extensions like RISE for slides, ExecuteTime
    to make it easy to time the execution of cells etc.
    * you can now call the tcapy Python library on your computer from your Python scripts
        * be sure to add `e:\cuemacro\tcapy` to your `PYTHONPATH` so it can find the tcapy library 
        * you can do this globally or by adding the following to the start of your Python script
        
            ```import sys
            import os
            tcapy_path = 'e:/cuemacro/tcapy'
            sys.path.insert(0, tcapy_path)
          
    * make sure to activate the `py37tca` conda environment if you want to use tcapy, you can do this by running
            
            conda activate py37tca 
    
        in your Anaconda prompt or you can run `e:\cuemacro\tcapy\batch_scripts\windows\installation\activate_python_environment.bat`
        which will activate it and also add the tcapy folder to your `PYTHONPATH`
        
    * to create PDF reports from tcapy output on Windows you'll need to separately install wkhtmltopdf and weasyprint, 
    which are converters for HTML to PDF, if these aren't installed PDF functionality in tcapy won't work
        * [wkhtmltopdf installation guide on Windows](https://github.com/JazzCore/python-pdfkit/wiki/Installing-wkhtmltopdf)
            * also requires adding the `wkhtmltopdf/bin folder` to your Windows path
        * [weasyprint installation guide on Windows](https://weasyprint.readthedocs.io/en/latest/install.html#windows)
            * also requires downloading and installation of GTK64, which is detailed in the above instructions
            * in particular the section describing "GTK+ 64 Bit Installer"     
    
4. install Linux using Microsoft's own Windows subsystem for Linux (WSL) and then install tcapy on WSL/Linux (step 2) and then 
install tcapy directly on Windows (step 4)
    * this gives you the ability to utilise some of the Linux specific features of tcapy, which may not
    be fully supported on Windows
    * at the same time you can call tcapy programmatically from Windows
        * so we can interact with Windows applications such as Excel (eg. using xlwings)
        * you still use the Linux supported features (which can run on your WSL Linux instances)
            * to speed up computations using Celery (via Redis as a message broker and Memcached as a results backend)
            * use Redis to cache trade/order and market data
            * to host the web app via nginx web server and gunicorn

We would generally recommend option 4 (install Linux using WSL, tcapy on WSL/Linux and then install tcapy directly on Windows), 
and we have been testing that. 

Whilst option 3 (install tcapy directly on Windows) is feasible, note, that doing this might 
make it difficult to run certain features such as Celery which is not fully supported. We have not tested other functionality 
such as the use of [nginx for Windows](http://nginx.org/en/docs/windows.html) to host the web GUI of tcapy directly on Windows.

# Mismatches in Python libraries when running on multiple OS

If you end up running for example tcapy on Windows (say to use in xlwings) but with a backend in Docker or Linux directly, 
you need to make sure that Python versions of libraries are relatively similar, if you want to run things in parallel. 
If you use the conda YML files and
use requirements.txt with Docker it should be fine in general.

Some the major issues with mismatching can be using different versions of Celery (and its dependencies like kombu) 
as well as pyarrow on Windows/Linux/Docker, which can cause issues when caching data/sending back and forth from Celery.

# Running tcapy on Linux

After tcapy installation it is recommended you restart you computer.

In order to start tcapy, we need to first run `cd /home/tcapyuser/cuemacro/tcapy/batch_scripts/linux/` then run
`./restart_db.sh` on Linux/WSL,  which will restart all the default databases and caching engines (also flushing the caches):

* Arctic/MongoDB - for storing tick data
* MySQL - for storing trade/order data
* Redis - for caching tick and trade/order data and for use as a message broker with Celery
* Memcached - for a results backend for Celery

* You can may need to edit this file, if
    * you use different database types (eg. InfluxDB or KDB for market tick data) 
    * or your databases are running on different servers (and you don't need to run locally)

Once all the databases/caches have been started, we can run `./restart_tcapy.sh` from the same folder.
This will do several things, which includes:

* Change the Python environment to `py37tca` under `conda` (although `virtualenv` environments are also supported)
* Start server web apps:
    * `http://localhost:9500/tcapy/` - main webapp
    * `http://localhost:9500/tcapyboard/` - simpler webapp where you can upload a trade CSV and get TCA output
    * `http://localhost:9500/tcapyapi/` - RESTful API endpoint
* Start Celery for distributed computation, which can also be accessed programmatically
    * If we do TCA on multiple assets, each asset is sent to a different Celery worker for computation
    * Celery also distributes the loading of data from the databases/caches by using different Celery workers

If you change `use_multithreading` to `False` in `constantscred.py` you can avoid using Celery and the backend, which reduces the number
of dependencies and is often easier to setup, as you won't need to run Celery workers. 

For Windows, if you want to kick off Celery workers for newer versions (4.x), it isn't supported, although there is a workaround 
(see https://www.distributedpython.com/2018/08/21/celery-4-windows/) - so we'd recommend avoid attempting to kick off the Celery
backend on Windows. Note, that calculations can be quite slow if Celery is not used (it will also reduce the caching opportunities)/

# Amazon Linux onpremises

If you'd like to run tcapy on the cloud, it is likely that you might want to use Amazon Linux. It is possible
to run this locally using a virtual machine, if you're trying to do testing, before deploying to AWS. Below are instructions:

* You can download it run a number of virtual machines applications including VMware and VirtualBox
* Running Amazon Linux 2 as a virtual machine onpremises https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-linux-2-virtual-machine.html
* Setting up a password on Amazon Linux local https://medium.com/shehuawwal/download-and-run-amazon-linux-2-ami-locally-on-your-virtualbox-or-vmware-b554a98dcb1c
* Adding sudo rights for a user https://serverfault.com/questions/599357/how-does-amazon-ec2-user-get-its-sudo-rights

# Archiving log files

It is recommended to archive the log files regularly, because they can become very big (many GB if continually appending)

# Installing new tcapy versions

If you install an updated version of tcapy, it is recommended you go through all the steps again, because there
often likely to be new (or updated) dependencies, which may include additional/updated Python libraries or external
applications

# Freeing up RAM

`echo 3 > /proc/sys/vm/drop_caches`, although this is not always a good idea https://serverfault.com/questions/597115/why-drop-caches-in-linux

    




