from datetime import datetime, time, date
from bson import ObjectId


def aggregate_data(db):

    firma_collection = db['ECR.firma']
    bon_collection = db['ECR.bon']
    bon_zilnic_collection = db['ECR.bon_zilnic']
    bon_zilnic_collection_parsed = db['ECR.bon_zilnic.parsed']
    produs_collection = db['ECR.produs']
    nui_collection = db['ECR.nui']

    new_collection = db['ECR.aggregated']

    firmas = list(firma_collection.find({}))

    for firma in firmas:
        cui = firma.get('_id')
        nui = nui_collection.find_one({'firma': firma.get('nume')})

        if nui:
            bon_zilnic_parsed = bon_zilnic_collection_parsed.find_one({'nui': int(nui.get('_id'))})
            print(nui.get('_id'), bon_zilnic_parsed, 'nui, bon_zilnic_parsed')

            if bon_zilnic_parsed:
                data = bon_zilnic_parsed.get('DATA')
                nr_z = str(bon_zilnic_parsed.get('nr_z')).zfill(4)

                bon_zilnic = bon_zilnic_collection.find_one({'_id': {'$regex': f'^{str(nui.get('_id'))}'},
                                                             'DATA': data, 'nr': nr_z})
                print(bon_zilnic, 'bon_zilnic')

                if bon_zilnic:
                    nr_bonuri = bon_zilnic.get('nr_bonuri')
                    bons = list(bon_collection.find({'DATA': data, 'Z': nr_z}))
                    print(bons, 'bons')

                    if len(bons) == int(nr_bonuri) and len(bons) != 0:
                        for bon in bons:
                            bon_id = bon.get('_id')
                            nr_bon = bon.get('BF')

                            produse = list(produs_collection.find({'bon_id': bon_id}))

                            for produs in produse:
                                new_document = {
                                    'cui': cui,
                                    'nui': nui.get('_id'),
                                    'DATA': data,
                                    'nr_z': nr_z,
                                    'nr_bon': nr_bon,
                                    'produs': {
                                        'nume': produs.get('nume'),
                                        'cantitate': produs.get('cantitate'),
                                        'valoare': produs.get('valoare'),
                                        'cota': produs.get('cota')
                                    }
                                }
                                print(new_document, 'new_document')
                                new_collection.insert_one(new_document)
                else:
                    break
            else:
                break
        else:
            break


def check_field_type(db):
    collection = db['ECR.bon']
    document = collection.find_one({})
    if document and isinstance(document['DATA'], datetime):
        print('The field has the correct Date data type.')
    else:
        print(type(document['DATA']))
        print('The field does not have the correct Date data type.')


def create_new_fields(id_field, include_nr_bon):
    new_fields = {
        'nui': id_field[:10],
        'hour': f"{id_field[18:20]}:{id_field[20:22]}:{id_field[22:24]}",
        'nr_z': id_field[24:28]
    }

    date_str = id_field[10:18]
    date_formats = ['%d%m%Y', '%Y%m%d']
    for date_format in date_formats:
        try:
            new_fields['DATA'] = datetime.strptime(date_str, date_format).strftime('%d-%m-%Y')
            break
        except ValueError:
            continue

    else:
        raise ValueError(f"Error parsing date: {date_str}")

    if include_nr_bon:
        new_fields['nr_bon'] = int(id_field[-4:])

    return new_fields


def custom_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime('%d-%m-%Y')
    elif isinstance(obj, time):
        return obj.strftime('%H:%M:%S')
    raise TypeError(f"Type {type(obj)} not serializable")


def convert_fields(doc):
    doc['nui'] = int(doc['nui'])
    doc['hour'] = datetime.strptime(doc['hour'], '%H:%M:%S').time()
    doc['nr_z'] = int(doc['nr_z'])

    if 'DATA' in doc:
        doc['DATA'] = datetime.strptime(doc['DATA'], '%d-%m-%Y')

    if 'data' in doc:
        doc['data'] = datetime.strptime(doc['data'], '%d-%m-%Y')

    if 'nr_bon' in doc:
        doc['nr_bon'] = int(doc['nr_bon'])

    return doc


def serialize_doc(doc):
    """
    Helper function to convert ObjectId to string
    """
    if isinstance(doc, list):
        for item in doc:
            for key, value in item.items():
                if isinstance(value, ObjectId):
                    item[key] = str(value)
    elif isinstance(doc, dict):
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                doc[key] = str(value)
    return doc
