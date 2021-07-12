import requests
import codecs
import time
import pandas as pd
import psycopg2
import threading
import os
import time
from datetime import datetime

"""
Llena la tabla SKU
"""
def getBasicSKUData(sku, pid, headers, catData):
    ErrorText = ''
    reintentar = True
    # Variables para guardar la informacion temporal
    category_id = -1
    department_id = -1
    brand_id = -1
    has_price = False
    total_inventory = 0
    sku_name = ""
    sku_images = []
    is_disabled = False
    skuimage_table= ""
    productatr_table = ""
    skuatr_table = ""
    sku_table = ""
    skuurl_table = ""
    is_active = ""
    # Atributos de los productos
    product_values_insert = []
    # Atributos del SKU
    sku_values_insert = []
    # Id de los atributos que se guardarán
    attributes_id = [
        2446, # Descripción amplia
        3044, # Tentrega5 
        3045, # Tentrega2 
        3046, # Tentrega3 
        3047, # Tentrega4 
        3048, # Tproducto 
        303, # Flags 
        304, # Flags Check
        381 # Temporada
    ]
    cantidad_reintentos = 0
    return_data = {}
    while reintentar:
        reintentar = False
        try:
            #Obtiene la categoria, el departamento y la marca del producto
            product_request = requests.get(
                url='https://cemacogt.vtexcommercestable.com.br/api/catalog/pvt/product/' + str(pid),
                headers=headers
            )
            if product_request.ok:
                product_json = product_request.json()
                category_id = str(product_json["CategoryId"])
                department_id = str(product_json["DepartmentId"])
                brand_id = str(product_json["BrandId"])
                product_url = 'https://cemaco.com/' + product_json["LinkId"] + '/p'
                product_url_request = requests.get(
                    url=product_url
                )
                return_data['Url'] = {
                    'ProductUrl': product_url,
                    'Sku': sku,
                    'ProductId': pid,
                    'StatusCode': product_url_request.status_code
                }
            elif product_request.status_code >= 500: 
                cantidad_reintentos = 4
                raise Exception("Error")
            else:
                raise Exception("Error")

            #Obtener precios del SKU
            price_request = requests.get(
                url= 'https://api.vtex.com/cemacogt/pricing/prices/' + str(sku),
                headers=headers
            )
            has_price = price_request.ok
            
            #Obtener inventario de cada sku
            inventory_request = requests.get(
                url='https://cemacogt.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/' + str(sku),
                headers=headers
            )
            if inventory_request.ok:
                inventory_json = inventory_request.json()
                for item in inventory_json["balance"]:
                    total_inventory += item["totalQuantity"]
            elif inventory_request.status_code >= 500: 
                cantidad_reintentos = 4
                raise Exception("Error")
            else:
                raise Exception("Error")

            #Obtener informacion del sku e imagenes
            sku_request = "https://cemacogt.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/" + str(sku)
            sku_full_response = requests.get(
                url = sku_request,
                headers = headers
            )
            if sku_full_response.ok:
                sku_json= sku_full_response.json()
                sku_name_aux = sku_json["ProductName"]
                sku_name = sku_name_aux.replace("'", "''")
                if(sku_json["IsActive"] == True):
                    is_active= "Activo"
                else:
                    is_active = "No Activo"
                for img in sku_json["Images"]:
                    sku_images.append({
                        "ImageUrl": str(img["ImageUrl"]),
                        "FileId": str(img["FileId"])
                    })
                # Lee los ProductSpecifications
                for item in sku_json["ProductSpecifications"]:
                    if item["FieldId"] in attributes_id:
                        field_name_aux = item["FieldName"].replace("'", "''")
                        product_values_insert.append({
                            "FieldId": str(item["FieldId"]),
                            "FieldName": field_name_aux,
                            "Value": item["FieldValues"][0].replace("'", "''")
                        })
                # Lee los SKUSpecifications 
                for item in sku_json["SkuSpecifications"]:
                    sku_fieldname_aux = item["FieldName"].replace("'", "''")
                    sku_values_insert.append({
                        "FieldId": str(item["FieldId"]),
                        "FieldName": sku_fieldname_aux,
                        "Value": item["FieldValues"][0].replace("'", "''"),
                        "ValueId": item["FieldValueIds"][0]
                    })
            elif sku_full_response.status_code >= 500: 
                cantidad_reintentos = 4
                raise Exception("Error")
            else:
                raise Exception("Error")

            return_data['SkuAttributes'] = sku_values_insert
            return_data['ProductValues'] = product_values_insert
            return_data['SkuImages'] = sku_images

            is_disabled = sku in catData

            return_data['SkuInfo'] = {
                'Sku': sku,
                'ProductId': pid,
                'SkuName': sku_name,
                'CategoryId': category_id,
                'DepartmentId': department_id,
                'BrandId': brand_id,
                'IsActive': is_active,
                'HasPrice': has_price,
                'Inventory': total_inventory,
                'Disabled': is_disabled
            }

            return_data['ConError'] = False

            return return_data
        except Exception as error:
            print(error)
            print("Reintentando el sku" + str(sku) + ", reintento " + str(cantidad_reintentos))
            time.sleep(30)
            reintentar = cantidad_reintentos < 3
            cantidad_reintentos = cantidad_reintentos + 1
        return {
            'ConError': True
        }