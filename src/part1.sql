create table if not exists customers (
    customer_id serial primary key,
    customer_name varchar(50) not null check (customer_name ~ '^[A-ZА-Я][a-zа-я\- ]+$'),
    customer_surname varchar(50) not null check (customer_surname ~ '^[A-ZА-Я][a-zа-я\- ]+$'),
    customer_primary_email varchar(50) not null check (customer_primary_email ~* '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
    customer_primary_phone varchar(15) not null check (customer_primary_phone ~ '^\+7[0-9]{10}$')
);

create table if not exists customer_cards (
    customer_card_id serial primary key,
    customer_id bigint,
    foreign key (customer_id) references customers(customer_id),
    unique (customer_id, customer_card_id)
);

create table if not exists transactions (
    transaction_id serial primary key,
    customer_card_id bigint not null,
    transaction_summ numeric not null check (transaction_summ > 0),
    transaction_date_time timestamp not null,
    transaction_store_id bigint not null,
    foreign key (customer_card_id) references customer_cards(customer_card_id)
);

create table if not exists sku_groups (
    group_id serial primary key,
    group_name text not null check (group_name ~ '^[A-Za-zА-Яа-я0-9\s\W]+$')
);

create table if not exists product_matrix (
    sku_id serial primary key,
    sku_name text not null check (sku_name ~ '^[A-Za-zА-Яа-я0-9\s\W]+$'),
    group_id bigint,
    foreign key (group_id) references sku_groups(group_id)
);

create table if not exists checks (
    transaction_id bigint,
    sku_id bigint,
    sku_amount numeric not null check (sku_amount > 0),
    sku_summ numeric not null check (sku_summ > 0),
    sku_summ_paid numeric not null check (sku_summ_paid > 0),
    sku_discount numeric check (sku_discount >= 0),
    foreign key (transaction_id) references transactions(transaction_id),
    foreign key (sku_id) references product_matrix(sku_id)
);

create table if not exists sales_stores (
    transaction_store_id bigint not null,
    sku_id bigint not null,
    sku_purchase_price numeric not null check (sku_purchase_price > 0),
    sku_retail_price numeric not null check (sku_retail_price > 0),
    foreign key (sku_id) references product_matrix(sku_id)
);

create table if not exists date_of_analysis_formation (
    analysis_formation timestamp
);

-- --------------------------procedure for import data from .tsv--------------------------------------------
create or replace procedure import_data_from_tsv
(in table_name varchar, in file_path text, in separator char) as
$import$
begin
    execute format('COPY %s FROM ''%s'' DELIMITER ''%s'';', table_name, file_path, separator);
end;
$import$
    language plpgsql;

-- ------------------------call this procedure to fill tables----------------------------------------------
-- !! you need to change file_path to the absolute path to tsv folder on yout computer !! -----------------

call import_data_from_tsv ('customers','/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/Personal_Data_Mini.tsv', E'\t');
call import_data_from_tsv ('customer_cards','/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/Cards_Mini.tsv', E'\t');
-- !! for import data with date in format not like timestamp, you need to set datestyle before it !! ------
set datestyle = 'ISO, DMY';
call import_data_from_tsv ('transactions','/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/Transactions_Mini.tsv', E'\t');
call import_data_from_tsv ('sku_groups','/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/Groups_SKU_Mini.tsv', E'\t');
call import_data_from_tsv ('product_matrix','/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/SKU_Mini.tsv', E'\t');
call import_data_from_tsv ('checks','/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/Checks_Mini.tsv', E'\t');
call import_data_from_tsv ('sales_stores','/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/Stores_Mini.tsv', E'\t');
call import_data_from_tsv ('date_of_analysis_formation','/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/Date_Of_Analysis_Formation.tsv', E'\t');

-- --------------------------procedure for export data to .tsv--------------------------------------------
create or replace procedure export_data_to_tsv
(in table_name varchar, in file_path text, in separator char) as
$import$
begin
    execute format('COPY %s TO ''%s'' DELIMITER ''%s'' CSV HEADER;', table_name, file_path, separator);
end;
$import$
    language plpgsql;

-- ------------------------call this procedure to export data from tables to .tsv files--------------------
-- !! you need to change file_path to the absolute path to tsv folder on yout computer !! -----------------

call export_data_to_tsv ('date_of_analysis_formation', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv_export/Date_Of_Analysis_Formation.tsv', E'\t');
call export_data_to_tsv ('customers', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv_export/Personal_Data.tsv', E'\t');
call export_data_to_tsv ('customer_cards', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv_export/Cards.tsv', E'\t');
call export_data_to_tsv ('transactions', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv_export/Transactions.tsv', E'\t');
call export_data_to_tsv ('sku_groups', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv_export/Groups_SKU.tsv', E'\t');
call export_data_to_tsv ('product_matrix', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv_export/SKU.tsv', E'\t');
call export_data_to_tsv ('checks', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv_export/Checks.tsv', E'\t');
call export_data_to_tsv ('sales_stores', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv_export/Stores.tsv', E'\t');


-- --------------------------procedure for export data to .csv--------------------------------------------
create or replace procedure export_data_to_csv
(in table_name varchar, in file_path text, in separator char) as
$import$
begin
    execute format('COPY %s TO ''%s'' DELIMITER ''%s'' CSV HEADER;', table_name, file_path, separator);
end;
$import$
    language plpgsql;

-- ------------------------call this procedure to export data from tables to .csv files--------------------
-- !! you need to change file_path to the absolute path to csv folder on yout computer !! -----------------

call export_data_to_csv ('date_of_analysis_formation', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Date_Of_Analysis_Formation.csv', E'\t');
call export_data_to_csv ('customers', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Personal_Data.csv', E'\t');
call export_data_to_csv ('customer_cards', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Cards.csv', E'\t');
call export_data_to_csv ('transactions', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Transactions.csv', E'\t');
call export_data_to_csv ('sku_groups', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Groups_SKU.csv', E'\t');
call export_data_to_csv ('product_matrix', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/SKU.csv', E'\t');
call export_data_to_csv ('checks', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Checks.csv', E'\t');
call export_data_to_csv ('sales_stores', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Stores.csv', E'\t');

-- --------------------------procedure for import data from .csv--------------------------------------------
create or replace procedure import_data_from_csv
(in table_name varchar, in file_path text, in separator char) as
$import$
begin
    execute format('COPY %s FROM ''%s'' DELIMITER ''%s'' CSV HEADER;', table_name, file_path, separator);
end;
$import$
    language plpgsql;

-- ------------------------call this procedure to fill tables from .csv files------__________--------------
-- !! you need to change file_path to the absolute path to csv folder on yout computer !! -----------------

call import_data_from_csv ('date_of_analysis_formation', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Date_Of_Analysis_Formation.csv', E'\t');
call import_data_from_csv ('customers', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Personal_Data.csv', E'\t');
call import_data_from_csv ('customer_cards', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Cards.csv', E'\t');
call import_data_from_csv ('transactions', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Transactions.csv', E'\t');
call import_data_from_csv ('sku_groups', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Groups_SKU.csv', E'\t');
call import_data_from_csv ('product_matrix', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/SKU.csv', E'\t');
call import_data_from_csv ('checks', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Checks.csv', E'\t');
call import_data_from_csv ('sales_stores', '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/csv/Stores.csv', E'\t');


