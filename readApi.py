import requests
import codecs
import time
import pandas as pd
import psycopg2
import threading
import os
import time
from datetime import datetime
import traceback

from requests.api import request

"""
Obtiene la informacion del SKU
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
    product_name = ""
    show_without_stock = False
    has_service = False
    has_attachment = False
    price = ""
    modelo = ""
    sku_images = []
    is_disabled = False
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
        381, # Temporada
        2701 # Video
    ]
    cantidad_reintentos = 0
    return_data = {}
    while reintentar:
        reintentar = False
        try:
            #Obtiene la categoria, el departamento y la marca del producto
            product_request = requests.get(
                url=f'https://cemacogt.vtexcommercestable.com.br/api/catalog/pvt/product/{pid}',
                headers=headers
            )
            if product_request.ok:
                product_json = product_request.json()
                category_id = product_json["CategoryId"]
                department_id = product_json["DepartmentId"]
                brand_id = product_json["BrandId"]
                product_name = product_json["Name"].replace("'", "''")
                show_without_stock = product_json["ShowWithoutStock"]
                product_url = f'https://cemaco.com/{product_json["LinkId"]}/p'
                product_url_request = requests.get(
                    url=product_url
                )
                return_data['Url'] = {
                    'ProductUrl': product_url,
                    'Sku': sku,
                    'ProductId': pid,
                    'StatusCode': product_url_request.status_code
                }
            else: raise Exception("Error")

            #Obtener precios del SKU
            price_request = requests.get(
                url= f'https://api.vtex.com/cemacogt/pricing/prices/{sku}',
                headers=headers
            )
            has_price = price_request.ok
            if price_request.ok:
                price_request_json = price_request.json()
                price = f'{price_request_json["costPrice"]}'
                if "fixedPrices" in price_request_json:
                    if "value" in price_request_json["fixedPrices"]:
                        price = f'{price_request_json["fixedPrices"]["value"]}'
            
            #Obtener inventario de cada sku
            inventory_request = requests.get(
                url=f'https://cemacogt.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/{sku}',
                headers=headers
            )
            if inventory_request.ok:
                inventory_json = inventory_request.json()
                for item in inventory_json["balance"]:
                    total_inventory += item["totalQuantity"]
            else:
                raise Exception("Error")

            #Obtener informacion del sku e imagenes
            sku_request = f"https://cemacogt.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/{sku}"
            sku_full_response = requests.get(
                url = sku_request,
                headers = headers
            )
            if sku_full_response.ok:
                sku_json= sku_full_response.json()
                sku_name_aux = sku_json["ProductName"]
                sku_name = sku_name_aux.replace("'", "''")
                modelo = sku_json["ManufacturerCode"]
                is_active = "Activo" if sku_json["IsActive"] == True else "No Activo"
                # Aqui obtener los services y Attachments
                if "Services" in sku_json:
                    for service in sku_json["Services"]:
                        if "ServiceTypeId" in service:
                            if service["ServiceTypeId"] == 1:
                                has_service = True
                if "Attachments" in sku_json:
                    for attachment in sku_json["Attachments"]:
                        if "Id" in attachment:
                            if attachment["Id"] == 1:
                                has_attachment = True
                imagenes_sku = f"https://cemacogt.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{sku}/file"
                requests_imagenes = requests.get(
                    url=imagenes_sku,
                    headers=headers
                )
                for img in sku_json["Images"]:
                    sku_images.append({
                        "ImageUrl": img["ImageUrl"],
                        "FileId": img["FileId"]
                    })
                if (requests_imagenes.ok):
                    requests_imagenes_json = requests_imagenes.json()
                    for img in requests_imagenes_json:
                        for sku_img in range(len(sku_images)):
                            if sku_images[sku_img]["FileId"] == img["ArchiveId"]:
                                sku_images[sku_img]["IsMain"] = img["IsMain"]
                                sku_images[sku_img]["Name"] = img["Name"]
                                sku_images[sku_img]["Label"] = img["Label"]
                # Lee los ProductSpecifications
                for item in sku_json["ProductSpecifications"]:
                    if item["FieldId"] in attributes_id:
                        field_name_aux = item["FieldName"]
                        product_values_insert.append({
                            "FieldId": item["FieldId"],
                            "FieldName": field_name_aux,
                            "Value": item["FieldValues"][0]
                        })
                # Lee los SKUSpecifications 
                for item in sku_json["SkuSpecifications"]:
                    sku_fieldname_aux = item["FieldName"]
                    sku_values_insert.append({
                        "FieldId": item["FieldId"],
                        "FieldName": sku_fieldname_aux,
                        "Value": item["FieldValues"][0],
                        "ValueId": item["FieldValueIds"][0]
                    })
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
                'ProductName': product_name,
                'ManufacturerCode': modelo,
                'CategoryId': category_id,
                'DepartmentId': department_id,
                'BrandId': brand_id,
                'IsActive': is_active,
                'HasPrice': has_price,
                'Price': price,
                'Inventory': total_inventory,
                'Disabled': is_disabled,
                'ShowWithoutStock': show_without_stock,
                'HasService': has_service,
                'HasAttachment': has_attachment
            }

            return_data['ConError'] = False

            return return_data
        except Exception as error:
            #traceback.print_exc()
            print(error)
            time.sleep(30)
            reintentar = cantidad_reintentos <= 2
            cantidad_reintentos = cantidad_reintentos + 1
    return {
        'ConError': True,
        'Sku': sku
    }