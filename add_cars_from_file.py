import pymongo
import datetime
import csv

client = pymongo.MongoClient('mongodb://user:password@ip_address/')

db = client['dealership']

cars = db['cars']
customers = db['customers']
purchases = db['purchases']

def add_car_data(filename):
    with open(filename, 'r') as file:
        columns = file.readline().split(',')
        file = csv.reader(file)
        columns_needed = ['Make', 'Model', 'Year', 'Engine HP', 'Vehicle Size', 'Vehicle Style', 'MSRP']
        indexs = list(filter(lambda x: columns[x].strip() in columns_needed , [i for i in range(len(columns))]))
        number_columns = {"MSRP", "Year", "Engine HP"}

        documents = []
        for row in file:
            document = {}
            for count, index in enumerate(indexs):
                data = row[index]
                if columns_needed[count] in number_columns:
                    try:
                        data = float(data)
                    except:
                        continue
                document[columns_needed[count]] = data

            documents.append(document)
        
        cars.insert_many(documents)add_car_data('data.csv')
