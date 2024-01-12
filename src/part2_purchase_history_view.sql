CREATE OR REPLACE VIEW purchase_history_view AS(
        Select Personal_information.Customer_ID,
            Transactions.Transaction_ID,
            Transactions.Transaction_DateTime,
            product_grid.group_id as group_id,
            SUM(checks.sku_amount * Stores.SKU_Purchase_Price) as group_cost,
            SUM(checks.SKU_Summ) as group_summ,
            SUM(checks.SKU_Summ_Paid) as group_summ_paid
        FROM Personal_information
            JOIN Cards on Cards.Customer_ID = Personal_information.Customer_ID
            JOIN Transactions on Transactions.Customer_Card_ID = Cards.Customer_Card_ID
            JOIN Checks on Transactions.Transaction_ID = Checks.Transaction_ID
            JOIN Product_grid on Checks.SKU_ID = Product_grid.SKU_ID
            JOIN Stores on Checks.SKU_ID = Stores.SKU_ID
            AND Transactions.Transaction_Store_ID = Stores.Transaction_Store_ID
        GROUP BY transactions.transaction_id,
            Personal_information.Customer_ID,
            product_grid.group_id
        ORDER BY transactions.transaction_id
    );
SELECT *
FROM purchase_history_view