import requests
import codecs
import time
import pandas as pd
import psycopg2
import threading
import os
import time

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
        304 # Flags Check
    ]
    cantidad_reintentos = 0
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
                product_url = 'https://beta.cemaco.com/' + product_json["LinkId"] + '/p'
                product_url_request = requests.get(
                    url=product_url
                )
                if product_url_request.status_code >= 500: raise Exception("Error")
                skuurl_table = "INSERT INTO productUrlStatus(product_id, sku, url, statusCode) VALUES (" + str(pid) + "," + str(sku) + ",'" + product_url + "'," + str(product_url_request.status_code) + ");"
            else: raise Exception("Error")

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
            else: raise Exception("Error")

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
            else: raise Exception("Error")

            for spec in sku_values_insert:
                skuatr_table += "INSERT INTO skuAttributes(sku, field_id, field_name, value_id, value_text) VALUES(" + str(sku) + "," + str(spec["FieldId"]) + ",'" + spec["FieldName"] + "'," + str(spec["ValueId"]) + ",'" + spec["Value"] + "');"
            for pv in product_values_insert:
                productatr_table += "INSERT INTO productAttribute(product_id, field_id, sku, field_name, field_value) VALUES (" + str(pid) + "," + str(pv["FieldId"]) + (",") + str(sku) + ",'" + pv["FieldName"] + "','" + pv["Value"] + "');"
            for img in sku_images:
                skuimage_table += "INSERT INTO skuImage(sku, file_id, image_url) VALUES (" + str(sku) + "," + str(img["FileId"]) + ",'" + img["ImageUrl"] + "');"

            is_disabled = sku in catData

            sku_table += "INSERT INTO sku(sku, product_id, sku_name, category_id, department_id, brand_id, is_active, has_price, inventory, disabled) VALUES(" + str(sku) + "," + str(pid) + ",'" + sku_name + "'," + str(category_id) + "," + str(department_id) + "," + str(brand_id) + ",'" + str(is_active) + "'," + str(has_price) + "," + str(total_inventory) + "," + str(is_disabled) + ");"

            return sku_table + productatr_table + skuatr_table + skuimage_table + skuurl_table
        except:
            print("Reintentando el sku" + str(sku) + ", reintento " + str(cantidad_reintentos))
            cantidad_reintentos = cantidad_reintentos + 1
            time.sleep(60)
            reintentar = cantidad_reintentos <= 10
        return ""