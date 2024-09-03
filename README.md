## BCG - Car Crash Case Study Analysis
---

Create a command-line executable Spark application that can be launched using spark-submit. The application should accept a configuration file as input and perform data processing and analytics tasks.

#### Dataset:

Data source consists of 6 csv files present in src/inputs folder. Input and output paths are config driven. Analytics code for which we want to execute the spark applicatiom is to be mentioned in the config file.

#### Description of Analytics to be performed : 
1.	Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

#### Structure of input(config.json) file : 
inputpath : Refers to local directory path where the repository is present. Need to update it with your local directory path. <br/>
output_path : Refers to path inside the src folder where you want to store the outputs.  <br/>
NOTE :  The output files for each analysis will be stored as a separate folder with analytics code as name within the given output path. "path-0000-........csv" file contains the actual output, ignore other metadata files inside the "analytics_code_x" folders.

```
{
    "input": {
        "inputpath": "/Users/ansum/OneDrive/Desktop/bcg_data_eng_case_study/src/inputs",
        "charges": "Charges_use.csv",
        "damages": "Damages_use.csv",
        "endorse": "Endorse_use.csv",
        "person": "Primary_Person_use.csv",
        "restrict": "Restrict_use.csv",
        "units": "Units_use.csv"
    },
    "output_path": "outputs",
    "local_source_path": "/Users/ansum/OneDrive/Desktop/bcg_data_eng_case_study/src",
    "analytics_code": "analytics_code_10"
}
```
#### Execution Process:

##### Prerequisite

* PySpark 3.5.2 or above
* Python 3.11.x

Check Spark is installed properly or not by running.
```
pyspark --version
```

We have to update the inout and output paths inside the config file as well as the analytics code for which we want the run the application. Once the changes are saved, we can execute the application. 

Run the application by using the following command.

```
spark-submit --master local[*] main.py --config config.json
```

#### Project Structure

```
├── src
│   ├── config.yml
│   ├── main.py
│   └── analytics
│   │   ├── __init__.py
│   │   └── body_style_analysis.py
│   │   ├── highest_accident_state_analysis.py
│   │   └── male_crashes_analysis.py
│   │   └── safe_crash_analysis.py
│   │   └── top_crash_vehicle_maker_analysis.py
│   │   └── top_speeding_vehicles_analysis.py
│   │   └── top_vehicle_crashes_analysis.py
│   │   └── top_zip_codes_analysis.py
│   │   └── two_wheeler_crashes_analysis.py
│   │   └── valid_license_hit_and_run_analysis.py
│   └── inputs
│   │   ├── Charges_use.csv
│   │   └── Damages_use.csv
│   │   └── Endorse_use.csv
│   │   └── Primary_Person_use.csv
│   │   └── Restrict_use.csv
│   │   └── Units_use.csv
│   └── outputs
│   │   ├── analytics_code_1
│   │   └── analytics_code_2
│   │   └── analytics_code_3
│   │   └── analytics_code_4
│   │   └── analytics_code_5
│   │   └── analytics_code_6
│   │   └── analytics_code_7
│   │   └── analytics_code_8
│   │   └── analytics_code_9
│   │   └── analytics_code_10
│   └── utils
│   │   └── logger.py
│   │   └── schemas.py
│   │   └── utils.py
├── README.md
├── LICENSE

```
NOTE : __pycache__ folders are created by Python to store the compiled bytecode of imported modules in the project. It Makes Importing Python Modules Faster. Ignore those folders.