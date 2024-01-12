-- Удаление таблиц и процедур первого парта
drop TABLE if exists Personal_information CASCADE;
drop TABLE if exists Cards CASCADE;
drop TABLE if exists Transactions CASCADE;
drop TABLE if exists Checks CASCADE;
drop TABLE if exists Product_grid CASCADE;
drop TABLE if exists Stores CASCADE;
drop TABLE if exists SKU_group CASCADE;
drop TABLE if exists Date_of_analysis_formation CASCADE;
drop Table if exists Stores_ID;
DROP PROCEDURE IF exists import_csv_to_db(text);
DROP PROCEDURE IF exists export_csv_from_db(text);
-- Удалние представлений созданных во втором парте
DROP VIEW if exists purchase_history_view;
DROP VIEW if exists customers_view;
DROP VIEW if exists groups_view;
DROP VIEW if exists periods_view;
-- Удаление ролей из part3
DROP ROLE administrator;
DROP ROLE visitor;
-- Удаление part6
DROP FUNCTION if EXISTS personal_offers_aimed_at_cross_selling(int, float, float, numeric, numeric);
-- Создание таблицы Personal_information
create table Personal_information (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(100) not null,
    Customer_Surname VARCHAR(100) not null,
    Customer_Primary_Email VARCHAR(100) not null,
    Customer_Primary_Phone VARCHAR(20) not null
);
-- Создание таблицы Cards
create table Cards (
    Customer_Card_ID INT PRIMARY KEY,
    Customer_ID INT,
    constraint fk_cards_customer_id foreign key (Customer_ID) references Personal_information(Customer_ID)
);
-- Создание таблицы SKU_group
create table SKU_group(
    GROUP_ID INT Primary KEY,
    GROUP_Name VARCHAR(100) not null
);
-- Создание таблицы Product_grid
create table Product_grid(
    SKU_ID INT PRIMARY KEY,
    SKU_Name VARCHAR(100) not null,
    GROUP_ID INT not null,
    constraint fk_product_grid_group_id foreign key (GROUP_ID) references SKU_group(GROUP_ID)
);
-- Создание промежуточной таблицы Stores_ID
create table Stores_ID(Transaction_Store_ID INT PRIMARY KEY);
-- Создание таблицы Stores
create table Stores(
    Transaction_Store_ID INT not null,
    SKU_ID INT not null,
    SKU_Purchase_Price FLOAT not null,
    SKU_Retail_Price FLOAT not null,
    constraint fk_stores_sku_id foreign key (SKU_ID) references Product_grid(SKU_ID),
    constraint fk_stores_store_id foreign key (Transaction_Store_ID) references Stores_ID(Transaction_Store_ID)
);
-- Создание таблицы Transactions
create table Transactions (
    Transaction_ID INT PRIMARY KEY,
    Customer_Card_ID INT not null,
    Transaction_Summ FLOAT not null,
    Transaction_DateTime TIMESTAMP DEFAULT current_timestamp not null,
    Transaction_Store_ID INT not null,
    constraint fk_transactions_customer_card_id foreign key (Customer_Card_ID) references Cards(Customer_Card_ID),
    constraint fk_transactions_store_id foreign key (Transaction_Store_ID) references Stores_ID(Transaction_Store_ID)
);
-- Создание таблицы Checks
create table Checks(
    Transaction_ID INT,
    SKU_ID INT,
    SKU_Amount FLOAT not null,
    SKU_Summ FLOAT not null,
    SKU_Summ_Paid FLOAT not null,
    SKU_Discount FLOAT not null,
    constraint fk_checks_transactions_id foreign key (Transaction_ID) references Transactions(Transaction_ID),
    constraint fk_checks_sku_id foreign key (SKU_ID) references Product_grid(SKU_ID)
);
-- Создание таблицы Date_of_analysis_formation
create table Date_of_analysis_formation(
    Transaction_DateTime TIMESTAMP DEFAULT current_timestamp not null
);
CREATE OR REPLACE PROCEDURE import_csv_to_db(IN delim text) LANGUAGE plpgsql AS $$ BEGIN EXECUTE FORMAT(
        'COPY Personal_information(Customer_ID, Customer_Name, Customer_Surname, Customer_Primary_Email, Customer_Primary_Phone) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/Personal_Data_Mini.csv'' delimiter %L csv;
COPY Cards(Customer_Card_ID, Customer_ID) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/Cards_Mini.csv'' delimiter %L csv;
COPY SKU_group(GROUP_ID, GROUP_Name) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/Groups_SKU_Mini.csv'' delimiter %L csv;
COPY Product_grid(SKU_ID, SKU_Name, GROUP_ID) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/SKU_Mini.csv'' delimiter %L csv;
COPY Stores_ID(Transaction_Store_ID) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/Stores_ID.csv'' delimiter %L csv;
COPY Stores(Transaction_Store_ID, SKU_ID, SKU_Purchase_Price, SKU_Retail_Price) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/Stores_Mini.csv'' delimiter %L csv;
COPY Transactions(Transaction_ID, Customer_Card_ID, Transaction_Summ, Transaction_DateTime, Transaction_Store_ID) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/Transactions_Mini.csv'' delimiter %L csv;
COPY Checks(Transaction_ID, SKU_ID, SKU_Amount, SKU_Summ, SKU_Summ_Paid, SKU_Discount) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/Checks_Mini.csv'' delimiter %L csv;
COPY Date_of_analysis_formation(Transaction_DateTime) FROM ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/import_сsv_files/Date_Of_Analysis_Formation.csv'' delimiter %L csv',
        delim,
        delim,
        delim,
        delim,
        delim,
        delim,
        delim,
        delim,
        delim
    );
END;
$$;
CREATE OR REPLACE PROCEDURE export_csv_from_db(IN delim text) LANGUAGE plpgsql AS $$ BEGIN EXECUTE FORMAT(
        'COPY Personal_information(Customer_ID, Customer_Name, Customer_Surname, Customer_Primary_Email, Customer_Primary_Phone) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/Personal_Data.csv'' delimiter %L csv;
COPY Cards(Customer_Card_ID, Customer_ID) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/Cards.csv'' delimiter %L csv;
COPY Transactions(Transaction_ID, Customer_Card_ID, Transaction_Summ, Transaction_DateTime, Transaction_Store_ID) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/Transactions.csv'' delimiter %L csv;
COPY Checks(Transaction_ID, SKU_ID, SKU_Amount, SKU_Summ, SKU_Summ_Paid, SKU_Discount) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/Checks.csv'' delimiter %L csv;
COPY Product_grid(SKU_ID, SKU_Name, GROUP_ID) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/SKU.csv'' delimiter %L csv;
COPY Stores(Transaction_Store_ID, SKU_ID, SKU_Purchase_Price, SKU_Retail_Price) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/Stores.csv'' delimiter %L csv;
COPY SKU_group(GROUP_ID, GROUP_Name) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/Groups_SKU.csv'' delimiter %L csv;
COPY Stores_ID(Transaction_Store_ID) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/Stores_ID.csv'' delimiter %L csv;
COPY Date_of_analysis_formation(Transaction_DateTime) TO ''/home/alexey/it/school21/bootcampSQL/SQL3_RetailAnalitycs_v1.0-1/src/export_сsv_files/Date_Of_Analysis_Formation.csv'' delimiter %L csv',
        delim,
        delim,
        delim,
        delim,
        delim,
        delim,
        delim,
        delim,
        delim
    );
END;
$$;
-- Вызов функции импорта
call import_csv_to_db(',');
-- Вызов функции эскпорта
call export_csv_from_db(',');