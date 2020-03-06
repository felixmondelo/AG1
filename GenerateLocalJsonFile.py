# Bajar y leer un fichero JSON
# Fichero JSON: Cámaras de tráfico
# Obtenido de : http://gobiernoabierto.valencia.es/es/data/

import urllib.request
import json
import pymongo
import pprint

def download_json(url):
    # Obtenemos el JSON de la URL y lo decodificamos
    contents = urllib.request.urlopen(url_data).read().decode()
    # Lo cargamos en un objeto JSON
    data = json.loads(contents)
    # ENTREGABLE Generar un JSON de los datos en un fichero en local
    with open("traffic_cameras.json", "w") as outfile:
        json.dump(data, outfile)
    return data

def get_mongodb_collection():
    # Iniciamos la conexion
    client = pymongo.MongoClient(
        "mongodb+srv://usuario1:WR4hcn4gLvkmioAN@prueba-emkv1.mongodb.net/test?retryWrites=true&w=majority")

    # Recuperamos la referencia de la base de datos
    db = client.bbdd_prueba

    # Obtenemos la referencia de la coleccion
    collection = db.traffic_cameras

    return collection

def get_camera(id, collection):
    camera = collection.find_one({"properties.idcamara": id})
    return camera

if __name__ == "__main__":
    # URL que contiene el JSON
    url_data = 'http://mapas.valencia.es/lanzadera/opendata/Tra-camaras/JSON'

    # Descargamos el JSON
    # data = download_json(url_data)

    # Obtenemos la coleccion de mongoDb
    collection = get_mongodb_collection()

    #db.traffic_cameras.delete_many({})

    # ENTREGABLE Insertar en MongoDB todos los datos sin utilizar un bucle. Definir colección y código
    #collection.insert_many(data["features"])

    # ENTREGABLE
    # Mediante una consulta a Mongo obtener unos campos concretos de una consulta buscando en un atributo un valor concreto. El valor tiene que ser un valor numérico.
    camera = get_camera("14205", collection)
    pprint.pprint(camera)

    # ENTREGABLE Hacer una consulta de Mongo de forma que nos devuelva, en función de un atributo, el top 3 ordenado. ¿Existe el atributo “coordinates” en el conjunto de datos? ¿Cómo se consulta?

    # ENTREGABLE Recorrer todos los registros de la colección y realizar una media en un atributo.
    average = 0
    for camera in collection.find():
        pprint.pprint(camera)
        average = average + camera["properties"]["angulo"]


    # Buscamos las cámaras
    #for documento in coleccion.find({"type": "Feature"}):
    #    pprint.pprint(documento)

    myquery = {"properties.angulo": "0.0"}

    for documento in collection.find(myquery):
        pprint.pprint(documento)

    for documento in collection.find({"geometry.type": "Point"}):
        pprint.pprint(documento)

    for documento in collection.find({"geometry.coordinates": {"$gt": 7 }}):
        pprint.pprint(documento)


