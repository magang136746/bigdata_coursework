import csv
from collections import defaultdict
from itertools import groupby
from concurrent.futures import ThreadPoolExecutor


# Step 1: Data Loading and Mapping
def mapper(passenger_data_file):
    passenger_data = defaultdict(int)

    with open(passenger_data_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            passenger_id = row[0]
            passenger_data[passenger_id] += 1

    return passenger_data.items()


# Step 2: Shuffling and Sorting
def shuffler(mapped_data):
    sorted_data = sorted(mapped_data, key=lambda x: x[0])
    return sorted_data


# Step 3: Reducing
def reducer(shuffled_data):
    max_flight_count = 0
    passenger_with_max_flights = None

    for passenger_id, flights in groupby(shuffled_data, key=lambda x: x[0]):
        flight_count = sum([flight_count for _, flight_count in flights])
        if flight_count > max_flight_count:
            max_flight_count = flight_count
            passenger_with_max_flights = passenger_id

    return passenger_with_max_flights, max_flight_count


# Step 4: Output
def output(passenger_with_max_flights, max_flight_count):
    print("Passenger with the highest number of flights:")
    print("Passenger ID:", passenger_with_max_flights)
    print("Flight Count:", max_flight_count)


# MapReduce Execution
def mapreduce_highest_flight_count(passenger_data_file):
    mapped_data = mapper(passenger_data_file)
    shuffled_data = shuffler(mapped_data)
    passenger_with_max_flights, max_flight_count = reducer(shuffled_data)
    output(passenger_with_max_flights, max_flight_count)


# 使用示例：
with ThreadPoolExecutor() as executor:
    executor.submit(mapreduce_highest_flight_count, 'D:\pycharm/bigdata_coursework/AComp_Passenger_data_no_error.csv')