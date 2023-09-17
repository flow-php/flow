# Test Data Generators

This directory contains scripts to generate test data for the Flwo PHP Parquet reader/writer.

### Prerequisites
 - Python 3.x installed
 - pip installed (Python Package Index)

### Installation

First go to the `src/lib/parquet/resources/python` directory and run the following command to install the required dependencies:

```shell
python3 -m venv parquet
source parquet/bin/activate
pip install -r requirements.txt
```

Once all dependencies are installed, you can run the following command to generate the test data:

```shell
python generators/lists.py
python generators/maps.py
python generators/orders.py
python generators/primitives.py
python generators/structs.py
```