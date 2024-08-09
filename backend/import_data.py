import json
from pymongo import MongoClient, errors
from bson import ObjectId


client = MongoClient(
    "mongodb+srv://zaporojan40:xl2PWid0jE0etk1P@cluster0.b8gpgd1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['BigDataDBComplete']


file_path = '../mongodb_completed/ECR.produs.json'
collection_name = file_path.split('/')
collection_name = collection_name[-1].replace('.json', '')
print(collection_name)


def insert_in_chunks(file_path, chunk_size=1000):
    try:
        collection = db[collection_name]
        with open(file_path, 'r') as f:
            data = json.load(f)

        if not isinstance(data, list):
            data = [data]

        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            try:
                collection.insert_many(chunk)
                print(f'Inserted chunk {i // chunk_size + 1}')
            except errors.BulkWriteError as bwe:
                print(f'Error inserting chunk {i // chunk_size + 1}: {bwe.details}')
                continue

    except Exception as e:
        print(f'Error: {e}')


# insert_in_chunks(file_path)
# print('Data inserted successfully.')


# For ECR.produs.json data
def preprocess_document(doc):
    if '_id' in doc and '$oid' in doc['_id']:
        doc['_id'] = ObjectId(doc['_id']['$oid'])
    return doc


def preprocess_and_insert_in_chunks(file_path, chunk_size=1000):
    collection = db[collection_name]
    with open(file_path, 'r') as f:
        data = json.load(f)

    if not isinstance(data, list):
        data = [data]

    data = [preprocess_document(doc) for doc in data]

    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        try:
            collection.insert_many(chunk)
            print(f'Inserted chunk {i // chunk_size + 1}')
        except Exception as e:
            print(f'Error inserting chunk {i // chunk_size + 1}: {e}')


# preprocess_and_insert_in_chunks(file_path)
# print("Data inserted successfully (for ECR.produs).")

