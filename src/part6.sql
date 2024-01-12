DROP FUNCTION if EXISTS personal_offers_aimed_at_cross_selling(int, float, float, numeric, numeric);
CREATE OR REPLACE function personal_offers_aimed_at_cross_selling(
        count_groups int,
        maximum_churn_index float,
        maximum_consumption_stability_index float,
        maximum_sku_share numeric(5, 2),
        allowable_margin_share numeric(5, 2)
    ) RETURNS TABLE (
        Customer_ID int,
        sku_name VARCHAR,
        offer_discount_depth float
    ) AS $$ WITH cte AS (
        SELECT Customer_id,
            find_group.group_id,
            Customer_Primary_Store,
            stores.sku_retail_price - sku_purchase_price as sku_max_marge,
            product_grid.sku_id,
            allowable_margin_share * (
                (stores.sku_retail_price - sku_purchase_price) / (stores.sku_retail_price)
            ) as Offer_Discount
        FROM (
                SELECT groups_view.Customer_id,
                    row_number() over (
                        PARTITION BY groups_view.Customer_ID
                        ORDER BY group_affinity_index DESC
                    ) as position,
                    groups_view.group_id,
                    Customer_Primary_Store
                FROM groups_view
                    join customers_view on groups_view.customer_id = customers_view.customer_id
                    join stores on stores.transaction_store_id = customers_view.customer_primary_store
                    join product_grid on product_grid.group_id = groups_view.group_id
                WHERE group_churn_rate <= maximum_churn_index
                    AND group_stability_index < maximum_consumption_stability_index
                GROUP BY groups_view.Customer_id,
                    groups_view.group_id,
                    Customer_Primary_Store,
                    groups_view.group_affinity_index
            ) find_group
            join stores on stores.transaction_store_id = find_group.Customer_Primary_Store
            join product_grid on product_grid.group_id = find_group.group_id
            and product_grid.sku_id = stores.sku_id
        WHERE position < count_groups
    )
SELECT result.Customer_id,
    product_grid.sku_name,
    CEILING(group_minimum_discount * 20.0) * 5 AS offer_discount_depth
FROM (
        SELECT Customer_id,
            group_id,
            Customer_Primary_Store,
            max(sku_max_marge),
            sku_id,
            (
                SELECT count(product_grid.sku_id) as dolya_sku
                From checks
                    join product_grid on product_grid.sku_id = checks.sku_id
                WHERE product_grid.sku_id = cte.sku_id
            ) / (
                SELECT count(transactions.transaction_id)
                From transactions
                    join checks on transactions.transaction_id = checks.transaction_id
                    join product_grid on product_grid.sku_id = checks.sku_id
                WHERE product_grid.group_id = cte.group_id
            )::float as dolya_marge,
            offer_discount
        FROM cte
        GROUP BY Customer_id,
            group_id,
            customer_primary_store,
            sku_id,
            offer_discount
    ) as result
    join product_grid on product_grid.group_id = result.group_id
    and product_grid.sku_id = result.sku_id
    join groups_view on groups_view.customer_id = result.customer_id
    and groups_view.group_id = result.group_id
WHERE dolya_marge <= maximum_sku_share $$ LANGUAGE SQL;
SELECT *
FROM personal_offers_aimed_at_cross_selling(5, 3, 0.5, 100, 30);