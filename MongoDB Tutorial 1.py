import pymongo
import datetime

client = pymongo.MongoClient('mongodb://user:password@server_ip/')

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

car = add_car('Mazda', 'CX5', 2020, 250, 45000)
customer = add_customer('Tim', 'Tech', 'Jul 20, 2000')
purchase = add_purchase(car.inserted_id, customer.inserted_id, 'Cash')

result = cars.update_many({'Make': 'Ford', 'Model': 'CX5'},
                            {'$set': {'Make': 'Ford', 'Model': 'Edge'}})
