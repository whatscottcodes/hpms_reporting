# HPMS Reporting Report

Scripts for creating csv files that can be used for reporting PACE information to HPMS. Includes data for enrollment, vaccinations, and med errors (create tab delimited txt file for med errors that can be uploaded).

## Requirements

All required packages are in the requirements.txt file. There is also an included environment.yml file for setting up a conda environment. Requires paceutils package to be installed in environment - use pip install e <local_path/to/pace_utils>.

### PaceUtils

Requires that the paceutils package to be installed. Can be found at http://github.com/whatscottcodes/paceutils.

Requires a SQLite database set up to the specifications in https://github.com/whatscottcodes/database_mgmt

## Use

Can be run as individual scripts or can run the run_hpms_reporting.py file. Can be run without any parameter - this will run for the last quarter. To run for other quarters, use the --q and --yr parameters to specific quarter and year to run the data for.

