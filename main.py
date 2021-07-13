import requests
import json
import threading
import time
import readApi
import ec2Stop
import psycopg2
import os
import multiprocessing
import pandas as pd
import concurrent.futures
import boto3
from dotenv import load_dotenv
from functools import partial
from itertools import repeat
from ec2_metadata import ec2_metadata

load_dotenv()

"""
Llena la tabla de categorias
"""
def getCategories():
    categories_table = "INSERT INTO categories (id_cat, level, level_1) VALUES (0, 0, 'Categorias');"
    id_cat = 0
    nivel = 0
    cats_file = open("categories.csv", 'r', encoding="utf-8")
    continuar_lectura = True

    while continuar_lectura:
        nivel1= ""
        nivel2= ""
        nivel3= ""
        line = cats_file.readline()
        items = line.split("|")
        if len(items) > 1:
            id_cat = items[0]
            nivel = items[1]
            nivel1 = items[2].strip()
            if len(items) == 4: 
                nivel2 = items[3].strip()
            if len(items) == 5:
                nivel3 = items[4].strip()
                nivel2 = items[3].strip()
            categories_table += "INSERT INTO categories (id_cat, level, level_1" + (",level_2 " if nivel2 != "" else "") + (",level_3" if nivel3 != "" else "") + ") VALUES (" + id_cat + ", " +    nivel + ", '" + nivel1 + "' " + (",'" + nivel2 + "' " if nivel2 != "" else "") + (",'" + nivel3 + "' " if nivel3 != "" else "") + ");" 
            #print(categories_table)
        if not line: 
            break
    cursor.execute(categories_table)

"""
Genera la linea para guardar en el archivo CSV
"""
def process_category(category, nivel, string_padre):
    
    category_id = category["id"]
    category_nombre = category["name"]
    tiene_hijos = category["hasChildren"]
    categories_array.append(category_id)
    cadena_funcion = str(category_id) + "|" + str(nivel) + string_padre + "|" + category_nombre  + "\n"
    string_padre = string_padre + "|" + category_nombre
    if (tiene_hijos):
        for child_item in category["children"]:
            cadena_funcion += str(process_category(child_item, nivel + 1, string_padre))
    return cadena_funcion

"""
Obtiene todos los productos y los SKU de VTEX (sin informacion)
"""
def getProductsSkus(from_value, to_value):
    encontrar = []
    repetir = True
    while repetir:
        url = 'https://cemacogt.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds?_from=' + str(from_value) + '&_to=' + str(to_value)
        products_response = requests.get(
            url = url,
            headers = headers
        )
        products_response.encoding = 'utf-8'
        if (products_response.ok):
            repetir = False
            products_json = products_response.json()
            if (from_value <= total_prod):
                for producto in products_json['data']:
                    for sku in products_json['data'][producto]:
                        encontrar.append({
                            "ProductId": producto,
                            "SKU": sku
                        })
        else:
            print("Hubo error, se esperaran 30 segundos")
            time.sleep(30)
    return encontrar

"""
Proceso que obtiene SKUs en grupos
"""
def process_product_sku(SkuProductList, RequestHeaders, DisabledSkus):
    try:
        sku_info = []
        for item in SkuProductList:
            try:
                query_result = readApi.getBasicSKUData(item['SKU'], item['ProductId'], RequestHeaders, DisabledSkus)
                if len(query_result) > 0:
                    sku_info.append(query_result)
            except: print("Hubo un error")
        
        reintentar = True
        cantidad_reintentos = 0
        while reintentar:
            try:
                connection = psycopg2.connect(
                    user=os.environ.get('POSTGRES_USER'), 
                    password=os.environ.get('POSTGRES_PASS'), 
                    host=os.environ.get('POSTGRES_HOST'), 
                    port=os.environ.get('POSTGRES_PORT'), 
                    database=os.environ.get('POSTGRES_DB')
                )
                cursor = connection.cursor()
                for sku in sku_info:
                    if not sku['ConError']:
                        try:
                            # Guarda la informacion del SKU
                            actual_sku_info = sku['SkuInfo']
                            sku_query = 'INSERT INTO sku(sku, product_id, sku_name, category_id, department_id, brand_id, is_active, has_price, inventory, disabled) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                            cursor.execute(sku_query, (actual_sku_info['Sku'], actual_sku_info['ProductId'], actual_sku_info['SkuName'], actual_sku_info['CategoryId'], actual_sku_info['DepartmentId'], actual_sku_info['BrandId'], actual_sku_info['IsActive'], actual_sku_info['HasPrice'], actual_sku_info['Inventory'], actual_sku_info['Disabled']))
                            connection.commit()

                            # Guarda la informacion del Link
                            actual_sku_url = sku['Url']
                            sku_url_query = 'INSERT INTO productUrlStatus(product_id, sku, url, statusCode) VALUES (%s, %s, %s, %s)'
                            cursor.execute(sku_url_query, (actual_sku_url['ProductId'], actual_sku_url['Sku'], actual_sku_url['ProductUrl'], actual_sku_url['StatusCode']))
                            connection.commit()

                            # Guarda las imagenes del SKU
                            for imagen in sku['SkuImages']:
                                imagen_query = 'INSERT INTO skuImage(sku, file_id, image_url) VALUES (%s, %s, %s)'
                                cursor.execute(imagen_query, (actual_sku_info['Sku'], imagen['FileId'], imagen['ImageUrl']))
                                connection.commit()

                            # Guarda las especificaciones del producto
                            for especificacion in sku['ProductValues']:
                                specification_query = 'INSERT INTO productAttribute(product_id, field_id, sku, field_name, field_value) VALUES (%s, %s, %s, %s, %s)'
                                cursor.execute(specification_query, (actual_sku_info['ProductId'], especificacion['FieldId'], actual_sku_info['Sku'], especificacion['FieldName'], especificacion['Value']))
                                connection.commit()

                            # Guarda las especificaciones de un SKU
                            for especificacion in sku['SkuAttributes']:
                                specification_query = 'INSERT INTO skuAttributes(sku, field_id, field_name, value_id, value_text) VALUES (%s, %s, %s, %s, %s)'
                                cursor.execute(specification_query, (actual_sku_info['Sku'], especificacion['FieldId'], especificacion['FieldName'], especificacion['ValueId'], especificacion['Value']))
                                connection.commit()
                        except (Exception, psycopg2.Error) as error:
                            connection.rollback()
                    else:
                        cursor.execute("INSERT INTO skus_error (sku) VALUES (%s)", (sku['Sku']))
                        connection.commit()
            except:
                print("Error insertando informacion en el thread " + str(os.getpid()))
                time.sleep(30)
                reintentar = cantidad_reintentos < 3
                cantidad_reintentos += 1
            finally:
                if connection:
                    cursor.close()
                    connection.close()
    except: print("Hubo un error")

if __name__ == '__main__':

    multiprocessing.freeze_support()
    terminate_process = multiprocessing.Process(target=ec2Stop.shutdown_4hours)
    terminate_process.start()
    #ELIMINAR LOS DATOS ACTUALES DE LA BASE DE DATOS 
    print('Limpiando la bd')
    connection = psycopg2.connect(
        user=os.environ.get('POSTGRES_USER'), 
        password=os.environ.get('POSTGRES_PASS'), 
        host=os.environ.get('POSTGRES_HOST'), 
        port=os.environ.get('POSTGRES_PORT'), 
        database=os.environ.get('POSTGRES_DB')
    )
    cursor = connection.cursor()
    try:
        cursor.execute("DELETE FROM categories")
        cursor.execute("DELETE FROM productattribute")
        cursor.execute("DELETE FROM producturlstatus")
        cursor.execute("DELETE FROM sku")
        cursor.execute("DELETE FROM skuattributes")
        cursor.execute("DELETE FROM skuimage")

        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error insertando un registro en la base de datos", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            connection.close()
    print('Obteniendo los SKUs deshabilitados')
    disabled_skus = []
    # Leer el excel de deshabilitados 
    cemaco_session = boto3.Session(
        aws_access_key_id=os.environ.get('AWS_ACCESS'),
        aws_secret_access_key=os.environ.get('AWS_SECRET'),
        region_name='us-east-1'
    )
    client = cemaco_session.resource('dynamodb')
    table = client.Table(os.environ.get('DYNAMO_TABLE'))

    scan_kwargs = {
        "FilterExpression": '#status = :status AND #valido = :valido',
        "ProjectionExpression": "Sku",
        "ExpressionAttributeValues": {
            ':status': 'DES',
            ':valido': 'Si'
        },
        "ExpressionAttributeNames": {
            '#status': 'Accion',
            '#valido': 'Valido'
        }
    }
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        temp_disabled_skus = response.get('Items')
        for item in temp_disabled_skus:
            disabled_skus.append(item['Sku'])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    print(len(temp_disabled_skus))
    print("Existen " + str(len(disabled_skus)) + " deshabilitados")

    #### Empieza script de categorias 

    # Headers para las peticiones a la API de VTEX
    headers = {
        'X-VTEX-API-AppKey': os.environ.get('VTEX_APP_KEY'),
        'X-VTEX-API-AppToken': os.environ.get('VTEX_APP_TOKEN')
    }
    lock_productos = threading.Lock()
    from_val = 0
    to_val = 1
    total_prod = 100000000000
    # Obtiene la cantidad de productos
    total_request = requests.get(
        url='https://cemacogt.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds?_from=1&_to=15',
        headers=headers
    )
    total_json = total_request.json()
    total_prod = total_json["range"]['total']
    continuar = True
    array_sku = []
    products_array = []

    #Obtiene las categorías

    url_categories = "https://cemacogt.vtexcommercestable.com.br/api/catalog_system/pub/category/tree/5"

    url_productos = "https://cemacogt.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitids?page=1&pagesize=1000000"

    categories_response = requests.get(
        url = url_categories,
        headers = headers
    )

    categories_response.encoding = 'uft-8'
    categoies_json = categories_response.json()

    categories_array = []
    categories_array.append(0)
    categories_array.append(1)
        
    cadena_csv = ""
    for category_item in categoies_json:
        cadena_csv += str(process_category(category_item, 1, ""))

    f = open("categories.csv", "w", encoding="utf-8")
    f.write(cadena_csv)
    f.close()

    print('Obteniendo las categorias')
    try:
        connection = psycopg2.connect(
            user=os.environ.get('POSTGRES_USER'), 
            password=os.environ.get('POSTGRES_PASS'), 
            host=os.environ.get('POSTGRES_HOST'), 
            port=os.environ.get('POSTGRES_PORT'), 
            database=os.environ.get('POSTGRES_DB')
        )
        cursor = connection.cursor()
        getCategories()

        connection.commit()
        count = cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        print("Error insertando un registro en la base de datos", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()


    request_limits = []
    print('Obteniendo productos y skus')
    while continuar:
        continuar = False
        if (from_val <= total_prod):
            from_val = to_val + 1
            to_val = to_val + 50
            request_limits.append({
                'Inferior': from_val,
                'Superior': to_val
            })
            continuar = True

    with concurrent.futures.ThreadPoolExecutor(max_workers=90) as executor:
        future_product_sku = {
            executor.submit(
                getProductsSkus, item['Inferior'], item['Superior']
            ) : item for item in request_limits
        }
        for future in concurrent.futures.as_completed(future_product_sku):
            if len(future.result()) > 0:
                for listado_item in future.result():
                    array_sku.append(listado_item)

    print('Procesando los SKUs')
    print("Existen " + str(len(array_sku)) + " SKUs")

    chunks = [array_sku[x : x + int(len(array_sku) / 100)] for x in range(0, len(array_sku), int(len(array_sku) / 100))]

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        future_product_sku = {
            executor.submit(
                process_product_sku, item, headers, disabled_skus
            ) : item for item in chunks
        }

    iid = ec2_metadata.instance_id
    ec2 = cemaco_session.client('ec2')
    ec2.terminate_instances(InstanceIds=[iid])

    terminate_process.join()