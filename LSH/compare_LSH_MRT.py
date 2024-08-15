from LSH.MRT_wit import build_MRT_tree_from_list
from LSH.LSH_wit import build_LSH_tree_from_list
from datetime import datetime
import pandas as pd
import struct
import time

ALL_SENSOR_NAME = ['OutdoorUV', 'OutdoorVisibleRay_Lux', 'OutdoorTemperature_C', 'OutdoorHumidity_pct',
               'OutdoorPressure_hPa', 'IndoorUV', 'IndoorVisibleRay_Lux',
               'IndoorHumidity_pct', 'IndoorPressure_hPa']
# SELECTED_SENSOR_NAME = ['OutdoorUV', 'OutdoorVisibleRay_Lux', 'OutdoorTemperature_C', 'OutdoorHumidity_pct',
#                'OutdoorPressure_hPa', 'IndoorUV']
SENSOR_NAME = ALL_SENSOR_NAME

DATASET_SELECT_LENGTH = 1024


def generate_data(measurement, field_key, field_value, timestamp):
    combined_bytes = struct.pack("8s32sdd", measurement.encode('utf-8'), field_key.encode('utf-8'), field_value, timestamp)
    # print(combined_bytes)
    combined_str = combined_bytes.hex()
    # print(measurement, field_key, field_value, timestamp, combined_bytes, combined_str, len(combined_bytes))
    # print(combined_str)
    return combined_str


def single_compare_from_self_generate_dataset(node_num):
    data = []
    for i in range(node_num):
        measurement = 'dataxx8b'
        field_key = 'OutdoorTemperature_Cxxxxxxxxx32b'
        # field_value = 24.5 
        field_value = 0.02 + i % 2
        # timestamp = i / 10
        timestamp = i + 1492819200.0
        combined_string1 = (generate_data(measurement, field_key, field_value, timestamp))
        print(measurement, field_key, field_value, timestamp)
        print(combined_string1)

        chunks = [combined_string1.encode()[j:j+8] for j in range(0, len(combined_string1.encode()), 8)]
        data.append(chunks)
    t1 = time.time()
    LSH_head, LSH_size = build_LSH_tree_from_list(data)
    t2 = time.time()
    MRT_head, MRT_size = build_MRT_tree_from_list(data)
    t3 = time.time()
    return LSH_size, MRT_size, t2-t1, t3-t2


def select_data_from_file():
    df = pd.read_csv('../dataset/RT-IFTTT/rawdata.csv', parse_dates=[0], header=0, nrows=DATASET_SELECT_LENGTH)
    df = df.rename(columns={df.columns[0]: 'timestamp'})
    df['timestamp'] = pd.to_datetime(df['timestamp']).apply(lambda x: x.timestamp())
    raw_data = df.values.tolist()  
    # print(raw_data)
    return raw_data


def single_compare_from_real_dataset(batch_size):
    LSH_sizes = []
    MRT_sizes = []
    LSH_costs = []
    MRT_costs = []
    raw_data = select_data_from_file()
    for sensor_name in SENSOR_NAME:
        sensor_idx = SENSOR_NAME.index(sensor_name) + 1
        index = 0
        batch = []
        while index < len(raw_data):
            measurement = 'RT-IFTTT'
            field_key = sensor_name
            combined_string1 = (generate_data(measurement, field_key, raw_data[index][sensor_idx], raw_data[index][0]))
            chunks = [combined_string1.encode()[j:j+8] for j in range(0, len(combined_string1.encode()), 8)]
            batch.append(chunks)

            # print(measurement, field_key, raw_data[index][sensor_idx], raw_data[index][0])
            # print(combined_string1)
            index += 1
            if index % batch_size == 0:

                t1 = time.time()
                LSH_head, LSH_size = build_LSH_tree_from_list(batch)
                t2 = time.time()
                MRT_head, MRT_size = build_MRT_tree_from_list(batch)
                t3 = time.time()

                LSH_sizes.append(LSH_size)
                LSH_costs.append(t2 - t1)
                MRT_sizes.append(MRT_size)
                MRT_costs.append(t3 - t2)
                batch = []
        # print(sensor_name, sum(LSH_sizes) / len(LSH_sizes), sum(MRT_sizes) / len(MRT_sizes), sum(LSH_costs) / len(LSH_costs), sum(MRT_costs) / len(MRT_costs))
    # print(sum(LSH_sizes) / len(LSH_sizes), sum(MRT_sizes) / len(MRT_sizes), sum(LSH_costs) / len(LSH_costs), sum(MRT_costs) / len(MRT_costs))
    return sum(LSH_sizes) / len(LSH_sizes), sum(MRT_sizes) / len(MRT_sizes), sum(LSH_costs) / len(LSH_costs), sum(MRT_costs) / len(MRT_costs)


def cal_single_hash_cost(batch_size):
    LSH_sizes = []
    MRT_sizes = []
    LSH_costs = []
    MRT_costs = []
    raw_data = select_data_from_file()
    for sensor_name in ['OutdoorUV']:
        sensor_idx = SENSOR_NAME.index(sensor_name) + 1
        index = 0
        batch = []
        while index < batch_size:
            measurement = 'RT-IFTTT'
            field_key = sensor_name
            combined_string1 = (generate_data(measurement, field_key, raw_data[index][sensor_idx], raw_data[index][0]))
            chunks = [combined_string1.encode()[j:j+8] for j in range(0, len(combined_string1.encode()), 8)]
            batch.append(chunks)

            index += 1
            if index % batch_size == 0:

                t1 = time.time()
                LSH_head, LSH_size = build_LSH_tree_from_list(batch)
                t2 = time.time()
                MRT_head, MRT_size = build_MRT_tree_from_list(batch)
                t3 = time.time()

                LSH_sizes.append(LSH_size)
                LSH_costs.append(t2 - t1)
                MRT_sizes.append(MRT_size)
                MRT_costs.append(t3 - t2)
                batch = []
    return sum(LSH_sizes) / len(LSH_sizes), sum(MRT_sizes) / len(MRT_sizes), sum(LSH_costs) / len(LSH_costs), sum(MRT_costs) / len(MRT_costs)


if __name__ == "__main__":
    # lsh_size, mrt_size, lsh_cost, mrt_cost = cal_single_hash_cost(4)

    print("leaf num, LSH size, MRT size, LSH cost, MRT cost")

    batch_sizes = [1, 10, 20, 40, 80]
    # for batch_size in range(4,5):
    for batch_size in batch_sizes:
    # for batch_size in range(1, 16):
        lsh_size, mrt_size, lsh_cost, mrt_cost = single_compare_from_real_dataset(batch_size)
        # lsh_size, mrt_size, lsh_cost, mrt_cost = single_compare_from_self_generate_dataset(batch_size)
        print(f'{batch_size}, {round(lsh_size)}, {mrt_size}, {lsh_cost}, {mrt_cost}')

    # select_data_from_file(100)