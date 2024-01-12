CREATE OR REPLACE VIEW customers_view AS(
        SELECT DISTINCT need_two_atr.Customer_ID,
            Customer_Average_Check,
            customer_average_check_segment,
            Customer_Frequency,
            Customer_Frequency_segment,
            customer_inactive_period,
            customer_churn_rate,
            Customer_Churn_Segment,
            Freq.position as Customer_segment,
            favourite_store.Customer_Primary_Store
        FROM (
                SELECT *,
                    (customer_inactive_period / Customer_Frequency)::numeric(10, 4) as customer_churn_rate,
                    CASE
                        WHEN (customer_inactive_period / Customer_Frequency) < 2 then 'Low'
                        WHEN (customer_inactive_period / Customer_Frequency) < 5 then 'Medium'
                        else 'High'
                    END AS Customer_Churn_Segment
                FROM (
                        Select Personal_information.Customer_ID,
                            avg(Transactions.Transaction_Summ) as Customer_Average_Check,
                            CASE
                                WHEN 100 * row_number() over (
                                    ORDER BY avg(Transactions.Transaction_Summ) DESC
                                )::numeric(7, 2) / Count(*) over ()::numeric(7, 2) <= 10 then 'High'
                                WHEN 100 * row_number() over (
                                    ORDER BY avg(Transactions.Transaction_Summ) DESC
                                )::numeric(7, 2) / Count(*) over ()::numeric(7, 2) <= 35 then 'Medium'
                                else 'Low'
                            END AS customer_average_check_segment,
                            (
                                EXTRACT(
                                    DAY
                                    FROM (
                                            (
                                                max(Transactions.Transaction_DateTime) - min(Transactions.Transaction_DateTime)
                                            ) / COUNT(Transactions.Transaction_ID)
                                        )
                                ) + (
                                    EXTRACT(
                                        HOURS
                                        FROM (
                                                (
                                                    max(Transactions.Transaction_DateTime) - min(Transactions.Transaction_DateTime)
                                                ) / COUNT(Transactions.Transaction_ID)
                                            )
                                    )::numeric(10, 2) / 24
                                )
                            )::numeric(10, 2) as Customer_Frequency,
                            CASE
                                WHEN 100 * row_number() over (
                                    ORDER BY (
                                            max(Transactions.Transaction_DateTime) - min(Transactions.Transaction_DateTime)
                                        ) / COUNT(Transactions.Transaction_ID)
                                )::numeric(7, 2) / Count(*) over ()::numeric(7, 2) <= 10 then 'Often'
                                WHEN 100 * row_number() over (
                                    ORDER BY (
                                            max(Transactions.Transaction_DateTime) - min(Transactions.Transaction_DateTime)
                                        ) / COUNT(Transactions.Transaction_ID)
                                )::numeric(7, 2) / Count(*) over ()::numeric(7, 2) <= 35 then 'Occasionally'
                                else 'Rarely'
                            END AS Customer_Frequency_segment,
                            (
                                EXTRACT(
                                    DAY
                                    FROM (
                                            max(Date_Of_Analysis_Formation.Transaction_DateTime) - max(Transactions.Transaction_DateTime)
                                        )
                                ) + (
                                    EXTRACT(
                                        HOURS
                                        FROM (
                                                max(Date_Of_Analysis_Formation.Transaction_DateTime) - max(Transactions.Transaction_DateTime)
                                            )
                                    )::numeric(10, 2) / 24
                                )
                            )::numeric(10, 2) as customer_inactive_period
                        FROM Date_Of_Analysis_Formation,
                            Personal_information
                            JOIN Cards on Cards.Customer_ID = Personal_information.Customer_ID
                            JOIN Transactions on Transactions.Customer_Card_ID = Cards.Customer_Card_ID
                        GROUP BY Personal_information.Customer_ID,
                            Date_Of_Analysis_Formation.Transaction_DateTime
                        ORDER BY Customer_Frequency
                    ) as part_customers
            ) as need_two_atr
            JOIN (
                SELECT row_number() over () as position,
                    t.attribute AS first_,
                    r.attribute AS second_,
                    e.attribute AS third_
                FROM (
                        SELECT 'Low' AS attribute
                        UNION ALL
                        SELECT 'Medium' AS attribute
                        UNION ALL
                        SELECT 'High' AS attribute
                    ) AS t
                    CROSS JOIN (
                        SELECT 'Rarely' AS attribute
                        UNION ALL
                        SELECT 'Occasionally' AS attribute
                        UNION ALL
                        SELECT 'Often' AS attribute
                    ) AS r
                    CROSS JOIN (
                        SELECT 'Low' AS attribute
                        UNION ALL
                        SELECT 'Medium' AS attribute
                        UNION ALL
                        SELECT 'High' AS attribute
                    ) AS e
            ) as Freq on need_two_atr.customer_average_check_segment = Freq.first_
            AND need_two_atr.Customer_Frequency_segment = Freq.second_
            AND need_two_atr.Customer_Churn_Segment = Freq.third_
            join (
                SELECT check_last_three_stores.Customer_ID,
                    CASE
                        WHEN counti = 1 then last_stores [1]
                        else transaction_store_id
                    END as Customer_Primary_Store
                FROM (
                        SELECT Customer_ID,
                            array_agg(Transaction_Store_ID)::int [] as last_stores,
                            count(DISTINCT Transaction_Store_ID) as counti
                        FROM (
                                SELECT Personal_information.Customer_ID,
                                    Transactions.Transaction_DateTime,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY Personal_information.Customer_ID
                                        ORDER BY Transactions.Transaction_DateTime DESC
                                    ) AS rn,
                                    Transactions.Transaction_Store_ID
                                FROM Personal_information
                                    JOIN Cards on Cards.Customer_ID = Personal_information.Customer_ID
                                    JOIN Transactions on Transactions.Customer_Card_ID = Cards.Customer_Card_ID
                            ) subquery2
                        WHERE rn <= 3
                        GROUP BY Customer_ID
                    ) as check_last_three_stores
                    join (
                        SELECT distinct_customer.Customer_ID,
                            distinct_customer.Transaction_Store_ID
                        FROM (
                                SELECT Personal_information.Customer_ID,
                                    Transactions.Transaction_Store_ID,
                                    count(Transactions.Transaction_Store_ID) as count_transactions,
                                    max(Transaction_DateTime) as later_date,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY Personal_information.Customer_ID
                                        ORDER BY COUNT(Transactions.Transaction_Store_ID) DESC,
                                            max(Transaction_DateTime) DESC
                                    ) AS rn
                                FROM Personal_information
                                    JOIN Cards ON Cards.Customer_ID = Personal_information.Customer_ID
                                    JOIN Transactions ON Transactions.Customer_Card_ID = Cards.Customer_Card_ID
                                GROUP BY Personal_information.Customer_ID,
                                    Transactions.Transaction_Store_ID
                            ) distinct_customer
                        WHERE rn = 1
                    ) as distinct_customer on distinct_customer.Customer_ID = check_last_three_stores.Customer_ID
                GROUP BY check_last_three_stores.Customer_ID,
                    check_last_three_stores.last_stores,
                    check_last_three_stores.counti,
                    distinct_customer.transaction_store_id
            ) as favourite_store on favourite_store.Customer_ID = need_two_atr.Customer_ID
    );
SELECT *
FROM customers_view;