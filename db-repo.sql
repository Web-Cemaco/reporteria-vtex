DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS productAttribute CASCADE;
DROP TABLE IF EXISTS productUrlStatus CASCADE;
DROP TABLE IF EXISTS sku CASCADE;
DROP TABLE IF EXISTS skuAttributes CASCADE;
DROP TABLE IF EXISTS skuImage CASCADE;
DROP TABLE IF EXISTS marcas CASCADE;
DROP TABLE IF EXISTS skus_error CASCADE;

CREATE TABLE IF NOT EXISTS productUrlStatus(
	product_id int NOT NULL,
	sku INT NOT NULL,
	url varchar(500),
	statusCode int NOT NULL,
	PRIMARY KEY(product_id,sku)
);

CREATE TABLE IF NOT EXISTS productAttribute(
	product_id int NOT NULL,
	field_id int NOT NULL,
	sku int NOT NULL,
	field_name varchar(64),
	field_value text,
	PRIMARY KEY(product_id, field_id, sku)
);

CREATE TABLE IF NOT EXISTS skuAttributes(
	sku int NOT NULL,
	field_id int NOT NULL,
	field_name text NOT NULL,
	value_id int NOT NULL,
	value_text text NOT NULL,
	PRIMARY KEY(sku, field_id)
);

CREATE TABLE IF NOT EXISTS skuImage(
	sku int NOT NULL, 
	file_id int NOT NULL,
	image_url text NOT NULL,
	is_main boolean,
	nombre varchar(500),
	label varchar(500),
	PRIMARY KEY(sku, file_id)
);

CREATE TABLE IF NOT EXISTS categories(
	id_cat int NOT NULL,
	level int NOT NULL,
	level_1 varchar(100),
	level_2 varchar(100),
	level_3 varchar(100),
	PRIMARY KEY(id_cat)
);

CREATE TABLE IF NOT EXISTS sku(
	sku int NOT NULL,
	product_id int NOT NULL,
	product_name varchar(500),
	sku_name varchar(500),
	category_id int NOT NULL,
	department_id int NOT NULL,
	brand_id int NOT NULL,
	show_without_stock boolean,
	modelo varchar(500),
	is_active varchar(100), 
	has_price boolean,
	price decimal,
	tiene_service_empaque boolean,
	tiene_attachment_empaque boolean,
	inventory int,
	modal varchar(100),
	disabled boolean,
	PRIMARY KEY(sku, product_id)
);

CREATE TABLE IF NOT EXISTS marcas(
	id int not null,
	nombre varchar(500)
);

CREATE TABLE IF NOT EXISTS skus_error(
	sku INT NOT NULL PRIMARY KEY
);