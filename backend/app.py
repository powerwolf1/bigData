import os
import sys
import logging
import json

from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson.json_util import ObjectId, loads
from datetime import datetime
from utils import aggregate_data, create_new_fields, custom_serializer, convert_fields, serialize_doc
import config


app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.ERROR)

client = MongoClient(f'mongodb://{config.MONGO_USER}:{config.MONGO_PASS}@{config.MONGO_HOST}:{config.MONGO_PORT}/{config.MONGO_DB}?authSource=admin',
                     connectTimeoutMS=30000,  # 30 seconds
                     socketTimeoutMS=30000,  # 30 seconds
                     serverSelectionTimeoutMS=30000  # 30 seconds
                     )
db = client[config.MONGO_DB]


# client = MongoClient(
#     "mongodb+srv://zaporojan40:xl2PWid0jE0etk1P@cluster0.b8gpgd1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
# db = client['bigDataDB']


# Test connection and fetch one document
# collection = db['ECR.bon']
# document = collection.find_one()
# print(f'Document: {document}')


@app.route('/convert_data_to_timestamp', methods=['POST'])
def convert_data_to_timestamp():
    collection = request.json.get('collection')

    pipeline = [
        {
            "$addFields": {
                "timestamp": {
                    "$dateFromString": {
                        "dateString": "$DATA",
                        "format": "%d-%m-%Y"
                    }
                }
            }
        },
        {
            "$addFields": {
                "timestamp": {
                    "$toLong": "$timestamp"
                }
            }
        },
        {
            "$merge": {
                "into": collection,
                "whenMatched": "merge"
            }
        }
    ]

    db[collection].aggregate(pipeline)
    print('Converted successfully!')
    return {'message': "Converting processed successfully!"}


@app.route('/parsing_id_bon', methods=['GET'])
def parsing_id_bon():
    try:
        bon_collection = db['ECR.bon']
        data = bon_collection.find({})
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    def bon_id_parsing(bon):
        _id = bon['_id']
        try:
            return create_new_fields(id_field=_id, include_nr_bon=True)
        except IndexError as ex:
            print(f"An error occurred: {ex}")
            sys.exit(1)

    parsed_data = [bon_id_parsing(bon) for bon in data]
    converted_data = [convert_fields(doc) for doc in parsed_data]
    json_compatible_data = json.loads(json.dumps(converted_data, default=custom_serializer))

    # dir_path = '../mongodb-sample'
    # file_path = os.path.join(dir_path, 'parsed_ECR.bon.json')
    # os.makedirs(dir_path, exist_ok=True)
    #
    # try:
    #     with open(file_path, 'w') as file:
    #         json.dump(json_compatible_data, file, indent=4, default=custom_serializer)
    #         print("Parsed JSON file created successfully.")
    # except Exception as e:
    #     print(f"An error occurred during parsing JSON file: {e}")
    #     sys.exit(1)

    try:
        collection = db['ECR.bon.parsed']
        collection.insert_many(json_compatible_data)
        print("Data uploaded to MongoDB successfully")
    except Exception as e:
        print(f"An error occurred during uploading JSON file to MongoDB: {e}")
        sys.exit(1)

    return {'message': "Parsing processed successfully!"}


@app.route('/parsing_id_bon_zilnic', methods=['GET'])
def parsing_id_bon_zilnic():
    try:
        bon_zilnic_collection = db['ECR.bon_zilnic']
        data = bon_zilnic_collection.find({})
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    def bon_zilnic_id_parsing(bon_zilnic):
        _id = bon_zilnic['_id']

        try:
            return create_new_fields(id_field=_id, include_nr_bon=False)
        except Exception as ex:
            print(f"An error occurred during parsing collection: {ex}")
            sys.exit(1)

    parsed_data = [bon_zilnic_id_parsing(bon_zilnic) for bon_zilnic in data]
    converted_data = [convert_fields(doc) for doc in parsed_data]
    json_compatible_data = json.loads(json.dumps(converted_data, default=custom_serializer))

    try:
        collection = db['ECR.bon_zilnic.parsed']
        collection.insert_many(json_compatible_data)
        print("Data uploaded to MongoDB successfully")
    except Exception as e:
        print(f"An error occurred during uploading JSON file to MongoDB: {e}")
        sys.exit(1)

    return {'message': "Parsing processed successfully!"}


@app.route('/parsing_id_produs', methods=['GET'])
def parsing_id_produs():
    try:
        produs_collection = db['ECR.produs']
        data = produs_collection.find({})
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    def produs_id_parsing(produs):
        _id = produs['bon_id']

        try:
            return create_new_fields(id_field=_id, include_nr_bon=True)
        except IndexError as ex:
            print(f"An error occurred during parsing collection: {ex}")
            sys.exit(1)

    parsed_data = [produs_id_parsing(produs) for produs in data]
    converted_data = [convert_fields(doc) for doc in parsed_data]
    json_compatible_data = json.loads(json.dumps(converted_data, default=custom_serializer))

    try:
        collection = db['ECR.produs.parsed']
        collection.insert_many(json_compatible_data)
        print("Data uploaded to MongoDB successfully")
    except Exception as e:
        print(f"An error occurred during uploading JSON file to MongoDB: {e}")
        sys.exit(1)

    return {'message': "Parsing processed successfully!"}


@app.route('/data', methods=['GET'])
def get_data():
    collection_name = request.args.get('collection')
    date_from = request.args.get('from')
    date_to = request.args.get('to')
    limit = int(request.args.get('limit', 15000))
    skip = int(request.args.get('skip', 0))

    app.logger.debug(f"Received request for collection: {collection_name}, from: {date_from}, to: {date_to}")

    if not collection_name:
        return jsonify({"error": "Collection name is required"}), 400

    collection = db[collection_name]

    schema = []
    try:
        sample_document = collection.find_one()
        if sample_document:
            schema = list(sample_document.keys())
    except Exception as e:
        logging.error(f"Error retrieving schema: {e}")
        return jsonify({"error": "Error retrieving schema"}), 500

    match_stage = {}
    try:
        if date_from and date_to and 'DATA' in schema:
            from_date = datetime.strptime(date_from, '%d-%m-%Y')
            to_date = datetime.strptime(date_to, '%d-%m-%Y')
            from_timestamp = int(from_date.timestamp() * 1000)
            to_timestamp = int(to_date.timestamp() * 1000)

            match_stage = {
                "$match": {
                    "timestamp": {
                        "$gte": from_timestamp,
                        "$lte": to_timestamp
                    }
                }
            }

        pipeline = [match_stage] if match_stage else []
        pipeline.append({"$skip": skip})
        pipeline.append({"$limit": limit})

        data = list(collection.aggregate(pipeline))

        for doc in data:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        logging.debug(f"MongoDB Query: {data}")
        return jsonify(data)

    except Exception as e:
        logging.error(f"Error querying MongoDB: {e}")
        return jsonify({"error": "Error querying MongoDB"}), 500


@app.route('/filter_by_nui', methods=['GET'])
def filter_by_nui():
    collection = request.args.get('collection')
    firma = request.args.get('firma')
    nui_id = request.args.get('nui_id')
    date_from = request.args.get('from')
    date_to = request.args.get('to')

    logging.debug(f"Received request for filtering by NUI: {nui_id}")

    if not nui_id and not firma:
        return jsonify({"error": "Missing required fields: provide either 'firma' or 'nui_id'"}), 400

    try:
        nui_ids = []

        if firma:
            nui_docs = db['ECR.nui'].find({"firma": firma})
            nui_ids = [doc["_id"] for doc in nui_docs]

        if nui_id:
            nui_ids.append(nui_id)

        if not nui_ids:
            return jsonify({"error": "No NUI IDs found for the provided firma"}), 404

        regex_patterns = [f"^{nui_id}" for nui_id in nui_ids]

        if collection in ['ECR.bon.parsed', 'ECR.bon_zilnic.parsed']:
            match_stage = {
                "$and": [
                    {
                        "$or": [{"nui": {"$regex": regex}} for regex in regex_patterns],
                    }
                ]
            }
        else:
            match_stage = {
                "$and": [
                    {
                        "$or": [{"_id": {"$regex": regex}} for regex in regex_patterns],
                    }
                ]
            }

        if date_from and date_to:
            from_date = datetime.strptime(date_from, '%d-%m-%Y')
            to_date = datetime.strptime(date_to, '%d-%m-%Y')
            from_timestamp = int(from_date.timestamp() * 1000)
            to_timestamp = int(to_date.timestamp() * 1000)

            match_stage["$and"].append({
                "timestamp": {
                    "$gte": from_timestamp,
                    "$lte": to_timestamp
                }
            })

        projection = {
            "_id": 1,
            "DATA": 1
        }

        collection_count_aggregation = db[collection].aggregate([
            {"$match": match_stage},
            {"$project": projection},
            {"$group": {"_id": "$DATA", "count": {"$sum": 1}}}
        ])

        collection_count = list(collection_count_aggregation)

        return jsonify({
            "collection_count": collection_count
        }), 200

    except Exception as e:
        logging.error(f"Error querying MongoDB: {e}")
        return jsonify({"error": "Error querying MongoDB"}), 500


@app.route('/nr_z_reports', methods=['GET'])
def nr_z_reports():
    try:
        collection = request.args.get('collection')
        from_date = request.args.get('from')
        to_date = request.args.get('to')
        nr_z = request.args.get('nr_z')

        from_date = datetime.strptime(from_date, '%d-%m-%Y')
        to_date = datetime.strptime(to_date, '%d-%m-%Y')

        # Convert datetime objects to Unix timestamps in milliseconds
        from_timestamp = int(from_date.timestamp() * 1000)
        to_timestamp = int(to_date.timestamp() * 1000)

        logging.info(f"Fetching nr.Z reports from {from_timestamp} to {to_timestamp}")

        match_stage = {
            "$match": {
                "timestamp": {
                    "$gte": from_timestamp,
                    "$lte": to_timestamp
                }
            }
        }

        if nr_z:
            match_stage["$match"]["nr_z"] = nr_z

        pipeline = {}

        if collection in ['ECR.bon.parsed', 'ECR.bon_zilnic.parsed']:
            project_stage = {
                "$project": {
                    "_id": 0,
                    "nr_z": 1,
                    "DATA": 1
                }
            }
            pipeline = [match_stage, project_stage]
        elif collection in ['ECR.bon', 'ECR.bon_zilnic']:
            add_fields_stage = {
                "$addFields": {
                    "nr_z": {"$substr": ["$_id", 24, 4]}
                }
            }
            project_stage = {
                "$project": {
                    "_id": 0,
                    "nr_z": {"$substr": ["$_id", 24, 4]},
                    "DATA": 1
                }
            }
            pipeline = [add_fields_stage, match_stage, project_stage]

        # Aggregation pipeline
        nr_z_aggregation = db[collection].aggregate(pipeline)

        nr_z_data = list(nr_z_aggregation)
        logging.info(f"Retrieved data: {nr_z_data}")
        return jsonify({"nr_z_data": nr_z_data}), 200

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/update', methods=['POST'])
def update_data():
    collection_name = request.json.get('collection')
    document_id = request.json.get('id')
    update_fields = request.json.get('update_fields')

    logging.debug(f"Received update request for collection: {collection_name}, id: {document_id},"
                  f" update_fields: {update_fields}")

    if not collection_name or not document_id or not update_fields:
        return jsonify({"error": "Missing required fields"}), 400

    collection = db[collection_name]

    if isinstance(document_id, dict) and '$oid' in document_id:
        document_id = ObjectId(document_id['$oid'])
    elif isinstance(document_id, str) and ObjectId.is_valid(document_id):
        document_id = ObjectId(document_id)

    try:
        result = collection.update_one({"_id": document_id}, {"$set": update_fields})
        if result.matched_count == 0:
            return jsonify({"error": "Document not found"}), 404

        return jsonify({"message": "Document updated successfully"}), 200

    except Exception as e:
        logging.error(f"Error updating document: {e}")
        return jsonify({"error": "Error updating document"}), 500


@app.route('/delete', methods=['POST'])
def delete_data():
    collection_name = request.json.get('collection')
    document_id = request.json.get('id')

    logging.debug(f"Received delete request for collection: {collection_name}, id: {document_id}")

    if not collection_name or not document_id:
        return jsonify({"error": "Missing required fields"}), 400

    collection = db[collection_name]

    if isinstance(document_id, dict) and '$oid' in document_id:
        document_id = ObjectId(document_id['$oid'])
    elif isinstance(document_id, str) and ObjectId.is_valid(document_id):
        document_id = ObjectId(document_id)

    try:
        result = collection.delete_one({"_id": document_id})
        if result.deleted_count == 0:
            return jsonify({"error": "Document not found"}), 404
        return jsonify({"message": "Document deleted successfully"}), 200
    except Exception as e:
        logging.error(f"Error deleting document: {e}")
        return jsonify({"error": "Error deleting document"}), 500


@app.route('/add', methods=['POST'])
def add_data():
    collection_name = request.json.get('collection')
    new_document = request.json.get('new_document')

    logging.debug(f"Received add request for collection: {collection_name}, new_document: {new_document}")

    if not collection_name or not new_document:
        return jsonify({"error": "Missing required fields"}), 400

    collection = db[collection_name]

    try:
        result = collection.insert_one(new_document)
        return jsonify({"message": "Document added successfully", "id": str(result.inserted_id)}), 200
    except Exception as e:
        logging.error(f"Error adding document: {e}")
        return jsonify({"error": "Error adding document"}), 500


@app.route('/add_bulk', methods=['POST'])
def add_bulk_data():
    collection_name = request.json.get('collection')
    new_documents = request.json.get('new_documents')

    logging.debug(f"Received add bulk request for collection: {collection_name}, new_documents: {new_documents}")

    if not collection_name or not new_documents:
        return jsonify({"error": "Missing required fields"}), 400

    collection = db[collection_name]

    try:
        bson_documents = [loads(json.dumps(doc)) for doc in new_documents]
        result = collection.insert_many(bson_documents)
        return jsonify({"message": "Documents added successfully", "ids": [str(id) for id in result.inserted_ids]}), 200

    except Exception as e:
        logging.error(f"Error adding documents: {e}")
        return jsonify({"error": "Error adding documents"}), 500


@app.route('/schema', methods=['GET'])
def get_schema():
    collection_name = request.json.get('collection')
    logging.debug(f"Received request for schema in collection: {collection_name}")

    if not collection_name:
        return jsonify({"error": "Collection name is required"}), 400

    collection = db[collection_name]

    try:
        sample_document = collection.find_one()
        if not sample_document:
            return jsonify({"error": "No documents found in the collection"}), 404
        return jsonify({"fields": list(sample_document.keys())}), 200
    except Exception as e:
        logging.error(f"Error retrieving schema: {e}")
        return jsonify({"error": "Error retrieving schema"}), 500


@app.route('/tva_stats', methods=['GET'])
def get_tva_stats():
    collection_name = request.args.get('collection')
    date_from = request.args.get('from')
    date_to = request.args.get('to')

    logging.debug(f"Received request for TVA stats from: {date_from}, to: {date_to}")

    if not collection_name:
        return jsonify({"error": "Collection name is required"}), 400

    query = {}
    if date_from and date_to:
        from_date = datetime.strptime(date_from, '%d-%m-%Y')
        to_date = datetime.strptime(date_to, '%d-%m-%Y')
        from_timestamp = int(from_date.timestamp() * 1000)
        to_timestamp = int(to_date.timestamp() * 1000)
        query['timestamp'] = {'$gte': from_timestamp, '$lte': to_timestamp}

    fields = []

    if collection_name == 'ECR.bon':
        fields = ['totA', 'totB', 'totC', 'totD']
    elif collection_name == 'ECR.bon_zilnic':
        fields = ['total_a', 'total_b', 'total_c', 'total_d']

    pipeline = [
        {'$match': query},
        {
            "$addFields": {
                f"{fields[0]}": {"$toDouble": f"${fields[0]}"},
                f"{fields[1]}": {"$toDouble": f"${fields[1]}"},
                f"{fields[2]}": {"$toDouble": f"${fields[2]}"},
                f"{fields[3]}": {"$toDouble": f"${fields[3]}"}
            }
        },
        {
            '$group': {
                '_id': None,
                'total_totA': {'$sum': f'${fields[0]}'},
                'total_totB': {'$sum': f'${fields[1]}'},
                'total_totC': {'$sum': f'${fields[2]}'},
                'total_totD': {'$sum': f'${fields[3]}'}
            }
        }
    ]

    try:
        if collection_name in ['ECR.bon', 'ECR.bon_zilnic']:
            collection = db[collection_name]
            result = list(collection.aggregate(pipeline))
            logging.debug(f"Aggregation result: {result}")
            if result:
                return jsonify(result[0]), 200
            else:
                return jsonify({"total_totA": 0, "total_totB": 0, "total_totC": 0, "total_totD": 0}), 200

    except Exception as e:
        logging.error(f"Error aggregating TVA stats: {e}")
        return jsonify({"error": "Error aggregating TVA stats"}), 500


@app.route('/collection_counts', methods=['GET'])
def get_collection_counts():
    collections = db.list_collection_names()
    collection_counts = {}

    try:
        for collection_name in collections:
            collection_counts[collection_name] = db[collection_name].count_documents({})

        return jsonify(collection_counts), 200
    except Exception as e:
        logging.error(f"Error getting collection counts: {e}")
        return jsonify({"error": "Error getting collection counts"}), 500


@app.route('/sums_by_hour', methods=['GET'])
def sums_by_hour():
    collection_name = request.args.get('collection')
    date_from = request.args.get('from')
    date_to = request.args.get('to')

    if not collection_name:
        return jsonify({"error": "Collection name is required"}), 400

    try:
        collection = db[collection_name]

        from_date = datetime.strptime(date_from, '%d-%m-%Y')
        to_date = datetime.strptime(date_to, '%d-%m-%Y')
        from_timestamp = int(from_date.timestamp() * 1000)
        to_timestamp = int(to_date.timestamp() * 1000)

        logging.info(f"Fetching hourly transactions from {from_timestamp} to {to_timestamp}")

        pipeline = [
            {
                "$match": {
                    "timestamp": {
                        "$gte": from_timestamp,
                        "$lte": to_timestamp
                    }
                }
            },
            {
                "$addFields": {
                    "hour": {
                        "$toInt": {
                            "$substr": ["$ORA", 0, 2]
                        }
                    },
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {
                                "$toDate": "$timestamp"
                            }
                        }
                    },
                    "total_double": {
                        "$toDouble": "$total"
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "hour": "$hour",
                        "date": "$date"
                    },
                    "total_sum": {
                        "$sum": "$total_double"
                    }
                }
            },
            {
                "$sort": {
                    "_id.date": 1,
                    "_id.hour": 1
                }
            }
        ]

        result = list(collection.aggregate(pipeline))
        return jsonify(result), 200

    except Exception as e:
        logging.error(f"Error querying MongoDB: {e}")
        return jsonify({"error": "Error querying MongoDB"}), 500


@app.route('/sums_by_day_of_week', methods=['GET'])
def sums_by_day_of_week():
    try:
        collection = request.args.get('collection')
        from_date = request.args.get('from')
        to_date = request.args.get('to')

        from_date = datetime.strptime(from_date, '%d-%m-%Y')
        to_date = datetime.strptime(to_date, '%d-%m-%Y')

        # Convert datetime objects to Unix timestamps in milliseconds
        from_timestamp = int(from_date.timestamp() * 1000)
        to_timestamp = int(to_date.timestamp() * 1000)

        logging.info(f"Fetching sums by day of week from {from_timestamp} to {to_timestamp}")

        match_stage = {
            "$match": {
                "timestamp": {
                    "$gte": from_timestamp,
                    "$lte": to_timestamp
                }
            }
        }

        add_fields_stage = {
            "$addFields": {
                "date": {
                    "$dateFromString": {
                        "dateString": "$DATA",
                        "format": "%d-%m-%Y"
                    }
                }
            }
        }

        group_stage = {
            "$group": {
                "_id": {"day_of_week": {"$dayOfWeek": "$date"}},
                "total_sum": {"$sum": {"$toDouble": "$total"}}
            }
        }

        sort_stage = {
            "$sort": {"_id.day_of_week": 1}
        }

        pipeline = [match_stage, add_fields_stage, group_stage, sort_stage]
        results = list(db[collection].aggregate(pipeline))

        for result in results:
            result["_id"] = result["_id"]["day_of_week"]

        return jsonify(results), 200

    except Exception as e:
        logging.error(f"Error querying MongoDB: {e}")
        return jsonify({"error": "Error querying MongoDB"}), 500


@app.route('/filtered_bon_zilnic', methods=['GET'])
def filtered_bon_zilnic():
    try:
        collection = request.args.get('collection')
        from_date = request.args.get('from')
        to_date = request.args.get('to')
        nr = request.args.get('nr_b')

        from_date = datetime.strptime(from_date, '%d-%m-%Y')
        to_date = datetime.strptime(to_date, '%d-%m-%Y')

        # Convert datetime objects to Unix timestamps in milliseconds
        from_timestamp = int(from_date.timestamp() * 1000)
        to_timestamp = int(to_date.timestamp() * 1000)

        logging.info(f"Fetching filtered bon_zilnic from {from_timestamp} to {to_timestamp}")

        match_stage = {
            "$match": {
                "timestamp": {
                    "$gte": from_timestamp,
                    "$lte": to_timestamp
                }
            }
        }

        if nr:
            match_stage["$match"]["nr"] = nr

        project_stage = {
            "$project": {
                "_id": 1,
                "nr": 1,
                "numerar": 1,
                "card": 1,
                "DATA": 1,
                "total_vanzari": {"$toDouble": "$total_vanzari"},
                "ORA": 1
            }
        }

        pipeline = [match_stage, project_stage]
        results = list(db[collection].aggregate(pipeline))

        return jsonify(results), 200

    except Exception as e:
        logging.error(f"Error querying MongoDB: {e}")
        return jsonify({"error": "Error querying MongoDB"}), 500


@app.route('/daily_transactions', methods=['GET'])
def daily_transactions():
    try:
        collection = request.args.get('collection')
        from_date = request.args.get('from')
        to_date = request.args.get('to')

        from_date = datetime.strptime(from_date, '%d-%m-%Y')
        to_date = datetime.strptime(to_date, '%d-%m-%Y')

        from_timestamp = int(from_date.timestamp() * 1000)
        to_timestamp = int(to_date.timestamp() * 1000)

        logging.info(f"Fetching daily transactions from {from_timestamp} to {to_timestamp}")

        match_stage = {
            "$match": {
                "timestamp": {
                    "$gte": from_timestamp,
                    "$lte": to_timestamp
                }
            }
        }

        group_stage = {
            "$group": {
                "_id": {
                    "nr_bonuri": "$nr_bonuri",
                    "date": "$DATA"
                },
                "count": {"$sum": 1}
            }
        }

        project_stage = {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "nr_bonuri": "$_id.nr_bonuri",
                "count": 1
            }
        }

        sort_stage = {
            "$sort": {"date": 1}
        }

        pipeline = [match_stage, group_stage, project_stage, sort_stage]
        result = list(db[collection].aggregate(pipeline))

        return jsonify(result), 200

    except Exception as e:
        logging.error(f"Error querying MongoDB: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/delete_collection', methods=['POST'])
def delete_collection():
    collection_name = request.json.get('collection')

    if not collection_name:
        return jsonify({"error": "Collection name is required"}), 400

    try:
        db.drop_collection(collection_name)
        return jsonify({"message": f"Collection '{collection_name}' deleted successfully."}), 200
    except Exception as e:
        logging.error(f"Error deleting collection: {e}")
        return jsonify({"error": "Error deleting collection"}), 500


@app.route('/aggregate_data', methods=['GET'])
def aggregate_data_endpoint():
    try:
        aggregate_data(db)
    except Exception as e:
        logging.error(f"Error aggregating data: {e}")
        return jsonify({"error": "Error aggregating data"}), 500

    return {'message': "Parsing processed successfully!"}


@app.route('/get_produs_documents', methods=['GET'])
def get_produs_documents():
    try:
        results = list(db["ECR.produs"].find({}))
        results = [serialize_doc(result) for result in results]
        return jsonify(results), 200
    except Exception as e:
        logging.error(f"Error fetching all products: {e}")
        return jsonify({"error": "Error fetching all products"}), 500


@app.route('/get_bon_by_id', methods=['GET'])
def get_bon_by_id():
    bon_id = request.json.get('bon_id')

    if not bon_id:
        return jsonify({"error": "Bon id is required"}), 400

    try:
        results = db['ECR.bon'].find_one({'_id': bon_id})
        return jsonify(results), 200
    except Exception as e:
        logging.error(f"Error fetching bon by id: {e}")
        return jsonify({"error": "Error fetching bon by id"}), 500


import logging
from flask import jsonify, request

@app.route('/get_bon_zilnic', methods=['POST'])  # Use POST instead of GET
def get_bon_zilnic():
    try:
        # Log the raw data for debugging
        logging.debug(f"Raw request data: {request.data}")

        # Attempt to parse JSON
        nr_z = request.json.get('nr')
        DATA = request.json.get('DATA')
        total = request.json.get('total_vanzari')
        totA = request.json.get('total_a')
        totB = request.json.get('total_b')
        totC = request.json.get('total_c')
        totD = request.json.get('total_d')

        # Log received values
        print(f"Received values: nr={nr_z}, DATA={DATA}, total_vanzari={total}, total_a={totA}, total_b={totB}, total_c={totC}, total_d={totD}")

        if not nr_z or not DATA:
            logging.error("nr_z and DATA are required fields")
            return jsonify({"error": "nr_z and DATA are required fields"}), 400

        # Querying MongoDB
        query = {
            "nr": nr_z,
            "DATA": DATA,
            "total_vanzari": str(total),
            "total_a": str(totA),
            "total_b": str(totB),
            "total_c": str(totC),
            "total_d": str(totD)
        }

        print(f"MongoDB query: {query}")

        bon_zilnic = db["ECR.bon_zilnic"].find_one(query)

        if bon_zilnic:
            print("Bon Zilnic found")
            return jsonify(serialize_doc(bon_zilnic)), 200
        else:
            print("Bon Zilnic not found with the provided query")
            return jsonify({"error": "Bon zilnic not found"}), 404

    except Exception as e:
        logging.error(f"Error fetching bon zilnic: {e}")
        return jsonify({"error": "Error fetching bon zilnic"}), 500


@app.route('/create_bon_zilnic', methods=['POST'])
def create_bon_zilnic():
    bon_zilnic_data = request.json

    try:
        if '_id' in bon_zilnic_data and bon_zilnic_data['_id'] == '':
            bon_zilnic_data.pop('_id')

        collection = db['ECR.bon_zilnic']
        sample_document = collection.find_one()
        schema = sample_document.keys() if sample_document else []

        for field in schema:
            if field != '_id':
                bon_zilnic_data[field] = str(bon_zilnic_data.get(field, ''))

        print(f"Inserting bon zilnic: {bon_zilnic_data}")
        result = collection.insert_one(bon_zilnic_data)
        print('_id insert', str(result.inserted_id))
        return jsonify({"_id": str(result.inserted_id)}), 200
    except Exception as e:
        logging.error(f"Error creating bon zilnic: {e}")
        return jsonify({"error": "Error creating bon zilnic"}), 500


@app.route('/update_bon_zilnic', methods=['PUT'])
def update_bon_zilnic():
    bon_zilnic_data = request.json

    if not bon_zilnic_data or '_id' not in bon_zilnic_data or not bon_zilnic_data['_id']:
        return jsonify({"error": "Bon zilnic data with valid '_id' is required"}), 400

    _id = bon_zilnic_data.pop('_id')

    collection = db['ECR.bon_zilnic']
    sample_document = collection.find_one()
    schema = sample_document.keys() if sample_document else []

    for field in schema:
        if field != '_id':
            bon_zilnic_data[field] = str(bon_zilnic_data.get(field, ''))

    try:
        print(f"Inserting bon zilnic: {bon_zilnic_data}", 'huiniushenika2')
        result = collection.update_one({'_id': ObjectId(_id)}, {'$set': bon_zilnic_data})
        print(_id, '_id update')
        if result.matched_count:
            return jsonify({"success": True, "_id": _id}), 200
        else:
            return jsonify({"error": "No document found with the provided _id"}), 404
    except Exception as e:
        logging.error(f"Error updating bon zilnic: {e}")
        return jsonify({"error": "Error updating bon zilnic"}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
