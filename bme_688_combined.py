from bme68x import BME68X
import bme68xConstants as cnst
import bsecConstants as bsec
from datetime import datetime
import json
import sqlite3
import sys
import os
import redis
from statistics import median, mean
import re

app_path = os.path.join(os.path.dirname(__file__), '../../..')
sys.path.append(app_path)

from sensor_kit.configuration.manager import get_kit_settings
kit_settings = get_kit_settings()

LOCAL_DB_PATH = '/home/tempread/capstone2025-main/project/local_database.db'

#mode = kit_settings['nose_detection_mode']
mode = "cat"
if mode == "cat":
    CONFIG_NAME = 'dirty_clean_20240129354.config' #cat, 354
else:        
    CONFIG_NAME = 'air_basket_20250131323.config' #pinesol+lysol, 323

def read_conf(path: str):
    with open(path, 'rb') as conf_file:
        conf = [int.from_bytes(bytes([b]), 'little') for b in conf_file.read()]
        conf = conf[4:]
    return conf

def prob_gas_labeler(entry, gases, min_val, max_val):
    if mode == "cat":
        class_payload = {'litter': round(entry["gas_estimate_1"], 2),
                         'air': round(entry["gas_estimate_2"], 2),
                        'cleaning_solution': 0}
    else:
        class_payload = {'basket': round(entry["gas_estimate_1"], 2),
                         'air': round(entry["gas_estimate_2"], 2),
                         'cleaning_solution': 0}
    return class_payload

def detect_cleaning_event(calibrated, gas_prev, gas_base, clean_prev):
    gas_change = abs((calibrated - gas_prev) / gas_prev)
    
    if mode == "cat":
        percent = 0.01
    else: 
        percent = 0.10

    if gas_change >= percent:     
        clean = {'new_cleaning_event': True}
    elif calibrated <= (gas_base - gas_base*0.10):
        clean = {'new_cleaning_event': True}
    else:
        clean = {'new_cleaning_event': False}
        
    return clean

def detect_person_event(bVOC, bVOC_prev, bVOC_base):
    bVOC_change = abs((bVOC - bVOC_prev) / bVOC_prev)
    
    if bVOC_change >= 0.025:        #percent change for bVOC
        personActivity = {'new_person_event1': True}
    elif 1 >= bVOC >= (bVOC_base + bVOC_base*0.10):
        personActivity = {'new_person_event2': True}
    else: 
        personActivity = {'new_person_event': False}
        
    return personActivity
    
def detect_door_event(pressure, pressure_prev, pressure_base):
    pressure_change = abs((pressure - pressure_prev) / pressure_prev) 
    
    if pressure_change >= 0.000035:
        doorActivity = {'new_door_event1': True}
    elif pressure >= pressure_base + 0.00015 * pressure_base or pressure <= pressure_base + 0.00015 * pressure_base:
        doorActivity = {'new_door_event': True}
    else:
        doorActivity = {'new_door_event': False}    
    return doorActivity    


def extract_gas_names(filename):
    pattern = r'([^_]+)_([^_]+)_(\d{11})\.config'  # Cat regex pattern
    match = re.match(pattern, filename)
    
    if match:
        gases = [group for group in match.groups()[:-1]]  # Exclude the last group (datetime)
        return gases
    else:
        return None

    
def main():
    sqliteConnection1 = sqlite3.connect('/ng-sensor/sensor_kit/sensors/bme/local_database.db') #deletes table every time
    cursor1 = sqliteConnection1.cursor()
    cursor1.execute("DROP TABLE IF EXISTS DETAILS") 
    cursor1.close()
    
    gas_data = []
    bVOC_data = []
    pressure_data = []
    count = 0
    gas_prev = .2
    pressure_prev = 99800
    bVOC_prev = .5
    gas_base = .5
    pressure_base = 99800
    bVOC_base = .5
    gas_cal_data = []
    
    s = BME68X(cnst.BME68X_I2C_ADDR_HIGH, 0)
    
    cleaning_algorithm = read_conf(f"/ng-sensor/sensor_kit/sensors/bme/config/{CONFIG_NAME}")
    
    max_val = float(0.05) 
    min_val = float(0.0)
    
    print('Heater Profile:',CONFIG_NAME[-10:-7])
    s.set_bsec_conf(cleaning_algorithm)
 
    BSEC_SAMPLE_RATE_HIGH_PERFORMANCE = 0.055556
    BSEC_SAMPLE_RATE_LP = 0.33333

    s.set_sample_rate(BSEC_SAMPLE_RATE_HIGH_PERFORMANCE)
    s.subscribe_gas_estimates(2)        #change number for cat litter or cleaning soln

    print('\nSTARTING MEASUREMENT\n')
    
    # Instantiate Redis
    r_db=redis.Redis(host='localhost',port=6379,db=0)

    while(True):
        try:
            data = s.get_digital_nose_data()
        except Exception as e:
            print(e)
            main()
        if data:
            # Data from BME688
            entry = data[-1]
            
            gases = extract_gas_names(CONFIG_NAME)
            class_payload = prob_gas_labeler(entry, gases, 1,1)
            
            # Print to Terminal
            if mode == "cat":
                print("")
                print(f'Clean {entry["gas_estimate_1"]}\nDirty {entry["gas_estimate_2"]}')      #cat litter
                print("")
                cal_gas_estimate_2 = (entry["gas_estimate_2"] - min_val) / (max_val - min_val)
                #clean = detect_cleaning_event(cal_gas_estimate_1)
                print(f'Calibrated Dirty {1 - cal_gas_estimate_2}\nCalibrated Clean {cal_gas_estimate_2}')
                print("")
            else:
                print(f'Air {entry["gas_estimate_1"]}\nBasket {entry["gas_estimate_2"]}')
                print("")
                cal_gas_estimate_1 = (entry["gas_estimate_1"] - min_val) / (max_val - min_val)
                print(f'Calibrated Air {cal_gas_estimate_1}\nCalibrated Basket {1 - cal_gas_estimate_1}')
                print("")

            _time_worker = datetime.now()
            timestamp = _time_worker.strftime("%Y-%m-%d %H:%M:%S")  # Human-readable timestamp
            intymdhm = int(_time_worker.strftime("%Y%m%d%H%M"))  # Compact timestamp for easier sorting/comparison
            
            
            count += 1
            print(count)
            #print(gas_data)
            if count < 20:
                print("Warming Up Device")
                
            elif 20 <= count <= 40:
                gas_cal_data.append(cal_gas_estimate_2)
                gas_data.append(entry["gas_estimate_2"])
                pressure_data.append(entry["raw_pressure"])
                bVOC_data.append(entry["breath_voc_equivalent"])
                print("Calibrating Device")
                
            else:
                bVOC_base = mean(bVOC_data)
                bVOC_base = bVOC_base + .10 * bVOC_base     #baseline
                #pressure_base = mean(pressure_data)
                #pressure_base = pressure_base + .0001 * pressure_base     #baseline
                if count == 42:
                    gas_base = cal_gas_estimate_2
                #gas_base = mean(gas_cal_data)
                #print(f"gas cal = {gas_cal_data}")
                #gas_base = gas_base + .10 * gas_base     #baseline
                max_val = max(gas_data) + (max(gas_data)*.4)
                print("Ready to use")

            #print(f"Pressure baseline: {pressure_base}")
            print(f"bVOC baseline: {bVOC_base}")
            print(f"Gas Base: {gas_base}")
            print(f"Max_val: {max_val}")
            print("")
            #person = detect_person_event(payload.get("bVOCe"), bVOC_avg)
            #doorOpen = detect_door_event(payload.get("Pressure"), pressure_avg)
            
            #payload = {**payload, **class_payload, **clean}
            #print(payload)

            if count >= 2:
                clean = detect_cleaning_event(cal_gas_estimate_2, gas_prev, gas_base, clean)
                personActivity = detect_person_event(payload.get("bVOCe"), bVOC_prev, bVOC_base)
                #doorActivity = detect_door_event(payload.get("Pressure"), pressure_prev, pressure_base)
                
                gas_prev = cal_gas_estimate_2
                bVOC_prev = payload.get("bVOCe")
                #pressure_prev = payload.get("Pressure")
            else: 
                clean = {'new_cleaning_event': False}
                #doorActivity = {'new_door_event': False}
                personActivity = {'new_person_event': False}
                
                            
            # Constructing the payload with various environmental data and timestamps
            # This includes data like IAQ (Indoor Air Quality), temperature, humidity, etc.
            # Data is rounded where appropriate for consistency
            # this is a Hashmap
            if mode == "cat":
                payload = {
                "timestamp": timestamp,  # Human-readable timestamp DO NOT REMOVE*
                "ymdhm": intymdhm,  # Compact timestamp DO NOT REMOVE*
                "IAQ_Accuracy": entry["iaq_accuracy"],  # Accuracy of IAQ measurement
                "IAQ": round(entry["iaq"], 1),  # IAQ value, rounded to 1 decimal place
                "Temperature": round(entry["temperature"]-2, 1),  # Temperature, rounded to 1 decimal place
                "Humidity": round(entry["humidity"], 1),  # Humidity, rounded to 1 decimal place
                "Pressure": round(entry["raw_pressure"], 1),  # Pressure, rounded to 1 decimal place
                "Gas": entry["raw_gas"],  # Gas concentration
                "Status": entry["run_in_status"],  # Sensor status
                "eCO2": entry["co2_equivalent"],  # Estimated CO2
                "bVOCe": entry["breath_voc_equivalent"],  # Breath VOC equivalent
                "Clean_Percent": cal_gas_estimate_2,    #calibrated 1
                "Dirty_Percent": 1 - cal_gas_estimate_2,     #calibrated 2
                "Dirty_Litter": clean,
                "Cat_Activity": personActivity,
                "Clean_Soln_Percent": None,    
                "Air_Percent": None,     
                "clean_activity": None,
                "person_activity": None,
                "hash_key": f"BME688:{kit_settings['repo_dir']}/{timestamp}"  # Unique hash key for payload
                }
            else:
                payload = {
                "timestamp": timestamp,  # Human-readable timestamp DO NOT REMOVE*
                "ymdhm": intymdhm,  # Compact timestamp DO NOT REMOVE*
                "IAQ_Accuracy": entry["iaq_accuracy"],  # Accuracy of IAQ measurement
                "IAQ": round(entry["iaq"], 1),  # IAQ value, rounded to 1 decimal place*
                "Temperature": round(entry["temperature"]-2, 1),  # Temperature, rounded to 1 decimal place*
                "Humidity": round(entry["humidity"], 1),  # Humidity, rounded to 1 decimal place*
                "Pressure": round(entry["raw_pressure"], 1),  # Pressure, rounded to 1 decimal place
                "Gas": entry["raw_gas"],  # Gas concentration
                "Status": entry["run_in_status"],  # Sensor status
                "eCO2": entry["co2_equivalent"],  # Estimated CO2
                "bVOCe": entry["breath_voc_equivalent"],  # Breath VOC equivalent
                "Clean_Soln_Percent": cal_gas_estimate_1,    #calibrated 1
                "Air_Percent": 1 - cal_gas_estimate_1,     #calibrated 2
                "clean_activity": clean,
                "person_activity": personActivity,
                "Clean_Percent": None,  
                "Dirty_Percent": None,   
                "Dirty_Litter": None,
                "Cat_Activity": None,
                "hash_key": f"BME688:{kit_settings['repo_dir']}/{timestamp}"  # Unique hash key for payload DO NOT REMOVE*
            }

            payload1 = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), #human readable timestamp
            "Temperature": round(entry["temperature"]-2, 1), #temp rounded to 1 decimal place
            "Pressure": round(entry["raw_pressure"], 1), #pressure, rounded to 1 decimal place
            "bVOCe": entry["breath_voc_equivalent"], #breath VOC equivalent
            }
                
            #print(f"Door activity: {doorActivity}")
            print(f"Person Activity: {personActivity}")
            print(f"Clean Activity: {clean}")
            print("")
            payload = {**payload, **class_payload}
            print(payload1)
            
            
            # Storing the hash key in a Redis set for tracking purposes
            # This can be used to keep a list of all payloads that need to be synchronized with the cloud
            r_db.publish('bme_minute',json.dumps(payload))
            payload["hash_key"] = f"BME680:ng-sensor/{timestamp}"
            r_db.sadd('bme-cloud-sync-list', payload["hash_key"])
            
            # Storing the payload in Redis using the hash key
            # The payload is converted to a JSON string for storage
            r_db.set(payload["hash_key"], json.dumps(payload))


            #with open('/home/smartnose/capstone2025/DEMO.json', 'w') as file:
             #  json.dump(payload, file)

            try:
                sqliteConnection = sqlite3.connect('/ng-sensor/sensor_kit/sensors/bme/local_database.db')
                cursor = sqliteConnection.cursor()
                table = """ CREATE TABLE IF NOT EXISTS CALIBRATEDCAT (
                            Time VARCHAR(255) NOT NULL,
                            Temperature DECIMAL(3,1),
                            Pressure DECIMAL(7,1),
                            Humidity DECIMAL(3,1),
                            BVOC DECIMAL(7,1),
                            IAQ DECIMAL(7,1),
                            Gas_Estimation_1 DECIMAL(7,2),
                            Gas_Estimation_2 DECIMAL(7,2),
                            Cal_gas_estimate_1 DECIMAL(7,2),
                            Cal_gas_estimate_2 DECIMAL(7,2)
                            ); """ 
                            
                if mode == "cat":
                    cursor.execute(table) #create new table
                
                    cursor.execute('''INSERT INTO CALIBRATEDCAT (Time,Temperature,Pressure,Humidity,BVOC,IAQ,Gas_Estimation_1,Gas_Estimation_2,Cal_gas_estimate_1,Cal_gas_estimate_2) VALUES (?,?,?,?,?,?,?,?,?,?)''',   
                    (payload['timestamp'],payload['Temperature'],payload['Pressure'],payload['Humidity'],payload['bVOCe'],entry["iaq"],entry["gas_estimate_1"],entry["gas_estimate_2"],cal_gas_estimate_2,(1-cal_gas_estimate_2))) 
                    sqliteConnection.commit() #apply changes
                
                    #add in/take away last column for third gas estimation
                    sqliteConnection = sqlite3.connect('/ng-sensor/sensor_kit/sensors/bme/local_database.db')
                    cursor = sqliteConnection.cursor()
                    table = """ CREATE TABLE IF NOT EXISTS BASKETCAT (
                                Time VARCHAR(255) NOT NULL,
                                Temperature DECIMAL(3,1),
                                Pressure DECIMAL(7,1),
                                BVOC DECIMAL(7,1),
                                IAQ DECIMAL(7,1),
                                Gas_Estimation_1 DECIMAL(7,2),
                                Gas_Estimation_2 DECIMAL(7,2),
                                Cal_gas_estimate_1 DECIMAL(7,2),
                                Cal_gas_estimate_2 DECIMAL(7,2)
                                ); """ 
                            
                    cursor.execute(table) #create new table
            
                    cursor.execute('''INSERT INTO BASKETCAT (Time,Temperature,Pressure,BVOC,IAQ,Gas_Estimation_1,Gas_Estimation_2,Cal_gas_estimate_1,Cal_gas_estimate_2) VALUES (?,?,?,?,?,?,?,?,?)''',        #cleaning
                    (payload['timestamp'],payload['Temperature'],payload['Pressure'],payload['bVOCe'],entry["iaq"],entry["gas_estimate_1"],entry["gas_estimate_2"],cal_gas_estimate_2,(1-cal_gas_estimate_2)))    #cleaning
                    sqliteConnection.commit() #apply changes
                
                    print("Record inserted successfully into table") 
                    cursor.close()


                else:
                    cursor.execute(table) #create new table
                
                    cursor.execute('''INSERT INTO DETAILS (Time,Temperature,Pressure,BVOC,IAQ,Gas_Estimation_1,Gas_Estimation_2,Cal_gas_estimate_1,Cal_gas_estimate_2) VALUES (?,?,?,?,?,?,?,?,?)''',   
                    (payload['timestamp'],payload['Temperature'],payload['Pressure'],payload['bVOCe'],entry["iaq"],entry["gas_estimate_1"],entry["gas_estimate_2"],cal_gas_estimate_1,(1-cal_gas_estimate_1))) 
                    sqliteConnection.commit() #apply changes
                
                    #add in/take away last column for third gas estimation
                    sqliteConnection = sqlite3.connect('/ng-sensor/sensor_kit/sensors/bme/local_database.db')
                    cursor = sqliteConnection.cursor()
                    table = """ CREATE TABLE IF NOT EXISTS BASKET (
                                Time VARCHAR(255) NOT NULL,
                                Temperature DECIMAL(3,1),
                                Pressure DECIMAL(7,1),
                                BVOC DECIMAL(7,1),
                                IAQ DECIMAL(7,1),
                                Gas_Estimation_1 DECIMAL(7,2),
                                Gas_Estimation_2 DECIMAL(7,2),
                                Cal_gas_estimate_1 DECIMAL(7,2),
                                Cal_gas_estimate_2 DECIMAL(7,2)
                                ); """ 
                            
                    cursor.execute(table) #create new table
            
            
                    cursor.execute('''INSERT INTO BASKET (Time,Temperature,Pressure,BVOC,IAQ,Gas_Estimation_1,Gas_Estimation_2,Cal_gas_estimate_1,Cal_gas_estimate_2) VALUES (?,?,?,?,?,?,?,?,?)''',        #cleaning
                    (payload['timestamp'],payload['Temperature'],payload['Pressure'],payload['bVOCe'],entry["iaq"],entry["gas_estimate_1"],entry["gas_estimate_2"],cal_gas_estimate_1,(1-cal_gas_estimate_1)))    #cleaning

                
                    sqliteConnection.commit() #apply changes
                
                    table = """ CREATE TABLE IF NOT EXISTS FULL_DETAILS (
                                Time VARCHAR(255) NOT NULL,
                                Temperature DECIMAL(3,1),
                                Pressure DECIMAL(7,1),
                                Humidity DECIMAL(3,1),
                                BVOC DECIMAL(7,1),
                                IAQ DECIMAL(7,1),
                                Gas_Estimation_1 DECIMAL(7,2),
                                Gas_Estimation_2 DECIMAL(7,2)
                                ); """ 
                            
                    cursor.execute(table) #create new table
                
                    cursor.execute('''INSERT INTO FULL_DETAILS (Time,Temperature,Pressure,Humidity,BVOC,IAQ,Gas_Estimation_1,Gas_Estimation_2) VALUES (?,?,?,?,?,?,?,?)''',  
                    (payload['timestamp'],payload['Temperature'],payload['Pressure'],payload['Humidity'],payload['bVOCe'],entry["iaq"],entry["gas_estimate_1"],entry["gas_estimate_2"])) 
                    sqliteConnection.commit() #apply changes
                
                    print("Record inserted successfully into table") 
                    cursor.close()

            except sqlite3.Error as error:
                print("Failed to insert data into table", error)
            finally:
                if sqliteConnection:
                    sqliteConnection.close()
                    
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    
    
if __name__ == '__main__':
    main()
