import snap7
from snap7.util import *
import struct
import csv
import json
from dotenv import load_dotenv
import os
import influxdb_client, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


load_dotenv()


def connect_plc(ip, rack=0, slot=2):
    client = snap7.client.Client()
    client.connect(ip, rack, slot)
    return client

def disconnect_plc(client):
    client.disconnect()

def read_csv_file(measurements):
    with open(measurements, 'r') as f:
        csv_reader = csv.DictReader(f)
        # Converteer de csv_reader naar een lijst van dictionaries
        measurements = list(csv_reader)
    
    return measurements

def read_plc_data(client, details):
    data = client.db_read(details['db_nummer'], details['start_adres'], details['grootte'])
    # print(data)
    return interpret_data(data, details)

def write_to_influxdb(write_api, influxdb_bucket, data):
    return write_api.write(bucket=influxdb_bucket, record=data)

def interpret_data(data, details):
    if details['datatype'] == "REAL":
        waarde = get_real(data, 0)
        return round(waarde * float(details['multiplier']), 2)
    elif details['datatype'] == "INT":
        waarde = get_int(data, 0)
        return round(float(waarde) * float(details['multiplier']), 2)
    # Voeg hier meer datatypes toe indien nodig
    return None

if __name__ == "__main__":
    tijd = time.time_ns()
    # Laad omgevingsvariabelen PLC
    plc_address = os.getenv('PLC_ADDRESS', '127.0.0.1')
    plc_rack = int(os.getenv('PLC_RACK', 0))
    plc_slot = int(os.getenv('PLC_SLOT', 2))
    # Laad omgevingsvariabelen InfluxDB
    influxdb_token = os.getenv('INFLUXDB_TOKEN')
    influxdb_org = os.getenv('INFLUXDB_ORG')
    influxdb_address = os.getenv('INFLUXDB_ADDRESS')
    influxdb_bucket = os.getenv('INFLUXDB_BUCKET')

    csv_bestand = os.getenv('CSV_BESTAND_PAD', 'uitlezen.csv')

    # Connecteer met de PLC
    client = connect_plc(plc_address, plc_rack, plc_slot)

    # Connecteer met InfluxDB
    client_influx = InfluxDBClient(url=influxdb_address, token=influxdb_token, org=influxdb_org)
    write_api = client_influx.write_api(write_options=SYNCHRONOUS)

    # Lees de metingen uit het CSV bestand
    metingen = read_csv_file(csv_bestand)

    # Loop over de metingen en schrijf de data naar InfluxDB
    for row in metingen:
        measurement_type = row['measurement_type']
        equipment_id = row['equipment_id']
        locatie = row['locatie']
        equipment_type = row['equipment_type']
        nummer = row['nr']
        metingen = json.loads(row['metingen'])

        # Maak een InfluxDB Point object aan
        point = Point(measurement_type)
        point.tag("equipment_id", equipment_id)
        point.tag("location", locatie)
        point.tag("equipment_type", equipment_type)
        point.tag("nummer", nummer)
        point.time(tijd, WritePrecision.NS)
        # print(f"Equipment ID: {equipment_id}")
        for parameter, details in metingen.items():
            # Lees de data van de PLC
            value = read_plc_data(client, details)
            # print(f"Parameter: {parameter}, Value: {value}")
            point.field(parameter, value)
        
        write_to_influxdb(write_api, influxdb_bucket, point)

    disconnect_plc(client)
