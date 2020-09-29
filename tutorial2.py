# Async https://motor.readthedocs.io/en/stable/
# MongoDB Compass: https://www.mongodb.com/try/download/compass

# Embedding documents
# want to perform fewest read operations possible
# https://docs.mongodb.com/manual/tutorial/model-embedded-one-to-one-relationships-between-documents/
# Can add a database refrence

# Query Selectors
# https://docs.mongodb.com/manual/reference/operator/query/#query-selectors

# Code Download
# https://techwithtim.net/wp-content/uploads/2020/09/MongoDB-Tutorial.zip

# FIRST TUTORIAL

import pymongo
import datetime
import csv
import pprint

printer = pprint.PrettyPrinter()

client = pymongo.MongoClient('mongodb://tim:tim@172.105.17.246/')

db = client['dealership']

cars = db['cars']
customers = db['customers']
purchases = db['purchases']

def add_car(make, model, year, engine_HP, msrp):
    document = {
        'Make': make,
        'Model': model,
        'Year': year,
        'Engine HP': engine_HP,
        'MSRP': msrp,
        'Date Added': datetime.datetime.now()
    }
    return cars.insert_one(document)

def add_customer(first_name, last_name, dob):
    document = {
        'First Name': first_name,
        'Last Name': last_name,
        'Date of Birth': dob,
        'Date Added': datetime.datetime.now(),
    }
    return customers.insert_one(document)

def add_purchase(car_id, customer_id, method):
    document = {
        'Car ID': car_id,
        'Customer ID': customer_id,
        'Method': method,
        'Date': datetime.datetime.now()
    }
    return purchases.insert_one(document)

# END FIRST TUTORIAL

# Use this to add lots of data so we can have some intersting queries
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
        
        cars.insert_many(documents)

#cars.delete_many({})
#add_car_data('data.csv')

# QUERIES

# All cars with price less than 30,000
result = cars.find({'MSRP': {'$lte': 30000}}).limit(5)
#printer.pprint(list(result))

# The count of each car brand in inventory
result = cars.aggregate([
    {'$addFields': {
        'cheap': {'$cond': [{'$lte': ['$MSRP', 30000]}, True, False]}
        }
    }
])
#printer.pprint(list(result))

# How many cars are older than 2000
result = cars.count_documents({'Year': {'$lte': 2000}})
#print(result)

# Average price of all car brands
result = cars.aggregate([
    {
        '$group':
        {
            '_id': '$Make',
            'count': {'$sum': 1},
            'average price': {'$avg': '$MSRP'},
            'models': {'$addToSet': '$Model'}
        }
    }
])
printer.pprint(list(result))

# Car with max price
result = cars.aggregate([
    {
        '$group':
        {
            '_id': None,
            'max_price': {'$max': '$MSRP'},
        }
    }
])
#printer.pprint(list(result))

result = cars.find({}).sort([('MSRP', -1)]).limit(1)
#printer.pprint(list(result))

# CUSTOMER CARS PURCHASED
def get_customer_info():
    print('Please enter the details for the customer... ')
    first_name = input('First Name: ')
    last_name = input('Last Name: ')
    return first_name, last_name


while True:
    first, last = get_customer_info()
    customer_and_purchases = list(customers.aggregate([
        {'$match': 
            {'First Name': first, 'Last Name': last}
        }, 
        {'$lookup': {
            'from': 'purchases',
            'localField': '_id',
            'foreignField': 'Customer ID',
            'as': 'purchases'
            }
        }]))

    if len(customer_and_purchases) < 0:
        print('No results')
        continue

    for i, customer in enumerate(customer_and_purchases):
        print(f'{i+1}. {customer["First Name"]} {customer["Last Name"]}, {customer["Date of Birth"]}')

    while True:
        selection = input('Type the number of the customer you\' like to access: ')
        if selection.isdigit() and int(selection) in range(1,len(customer_and_purchases)+1):
            customer = customer_and_purchases[int(selection) - 1]
            break
        print('Invalid, try again...')

    print(f'Customer has purchased {len(customer["purchases"])} cars')
    for i, entry in enumerate(customer_and_purchases):
        print(f'{i+1}. {entry["purchases"]}')


