CREATE OR REPLACE VIEW periods_view AS(
        Select Customer_ID,
            GROUP_ID as group_id,
            MIN(Transaction_DateTime) as First_Group_Purchase_Date,
            MAX(Transaction_DateTime) as Last_Group_Purchase_Date,
            COUNT(DISTINCT checks.Transaction_ID) as group_purchase,
            (
                EXTRACT(
                    DAY
                    FROM (
                            (
                                MAX(Transaction_DateTime) - MIN(Transaction_DateTime)
                            ) / COUNT(DISTINCT purchase_history_view.Transaction_ID)
                        )
                ) + (
                    EXTRACT(
                        HOURS
                        FROM (
                                (
                                    MAX(Transaction_DateTime) - MIN(Transaction_DateTime)
                                ) / COUNT(DISTINCT purchase_history_view.Transaction_ID)
                            )
                    )::numeric(10, 2) / 24
                ) + 1
            )::numeric(10, 2) as GROUP_FREQUENCY,
            COALESCE(MIN(NULLIF(sku_discount / SKU_Summ, 0)), 0) AS Group_Min_Discount
        From purchase_history_view
            join checks on checks.Transaction_ID = purchase_history_view.Transaction_ID
        GROUP BY customer_id,
            GROUP_ID
    );
Select *
FROM periods_view