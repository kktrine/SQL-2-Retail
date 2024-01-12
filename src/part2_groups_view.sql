CREATE OR REPLACE FUNCTION fnc_group_Margin(
        in_customer_id BIGINT,
        in_group_id BIGINT,
        in_method INT,
        in_interval INTERVAL,
        in_count INT
    ) RETURNS NUMERIC LANGUAGE plpgsql AS $$
DECLARE group_margin NUMERIC;
BEGIN IF (in_method = 1) THEN group_margin := (
    SELECT sum(margin)
    FROM (
            SELECT SUM(Group_Summ_Paid) - SUM(Group_Cost) as margin
            FROM purchase_history_view
            WHERE customer_id = in_customer_id
                AND group_id = in_group_id
                AND transaction_datetime BETWEEN (
                    SELECT transaction_datetime
                    FROM date_of_analysis_formation
                ) - in_interval
                AND (
                    SELECT transaction_datetime
                    FROM date_of_analysis_formation
                )
        ) as a
);
ELSEIF (in_method = 2) THEN group_margin := (
    SELECT sum(margin)
    FROM (
            SELECT SUM(Group_Summ_Paid) - SUM(Group_Cost) as margin
            FROM purchase_history_view
            WHERE customer_id = in_customer_id
                AND group_id = in_group_id
            GROUP BY purchase_history_view.transaction_datetime
            ORDER BY transaction_datetime DESC
            LIMIT in_count
        ) as a
);
END IF;
RETURN group_margin;
END;
$$;
--
--
CREATE OR REPLACE VIEW groups_view AS (
        SELECT DISTINCT customer_id,
            group_id,
            (
                SELECT COUNT(
                        DISTINCT CASE
                            WHEN p2.group_id = p1.group_id THEN 1
                        END
                    )::float / COUNT(DISTINCT transaction_id)
                FROM purchase_history_view p2
                WHERE transaction_datetime BETWEEN first_group_purchase_date AND last_group_purchase_date
                    AND p1.customer_id = p2.customer_id
            ) AS group_affinity_index,
            (
                SELECT DATE_PART(
                        'day',
                        d1.transaction_datetime - p1.last_group_purchase_date
                    ) / p1.group_frequency::float
                FROM date_of_analysis_formation d1
            ) AS group_churn_rate,
            (
                SELECT AVG(date_difference) as group_stability_index
                FROM (
                        SELECT ABS (
                                EXTRACT(
                                    DAY
                                    FROM transaction_datetime - LAG(transaction_datetime) OVER (
                                            ORDER BY transaction_datetime
                                        )
                                ) - group_frequency
                            ) / group_frequency AS date_difference
                        FROM purchase_history_view pur
                            JOIN periods_view per ON per.customer_id = pur.customer_id
                            AND per.group_id = pur.group_id
                        WHERE pur.customer_id = p1.customer_id
                            AND pur.group_id = p1.group_id
                    ) subquery
            ) as group_stability_index,
            fnc_group_Margin(customer_id, group_id, 1, '365 days', 100) as group_margin,
            (
                (
                    SELECT COUNT (DISTINCT c.transaction_id)
                    FROM checks c
                        JOIN transactions t on c.transaction_id = t.transaction_id
                        JOIN cards ca ON t.customer_card_id = ca.customer_card_id
                        JOIN product_grid pr ON c.sku_id = pr.sku_id
                    WHERE ca.customer_id = p1.customer_id
                        AND pr.group_id = p1.group_id
                        AND c.sku_discount > 0
                ) / (
                    SELECT group_purchase
                    FROM periods_view p2
                    WHERE p2.group_id = p1.group_id
                        AND p2.customer_id = p1.customer_id
                )
            ) as group_discount_share,
            (
                SELECT group_min_discount
                FROM periods_view p2
                WHERE p2.group_id = p1.group_id
                    AND p2.customer_id = p1.customer_id
            ) as group_minimum_discount,
            (
                SELECT SUM(Group_Summ_Paid) / SUM(Group_Summ)
                FROM purchase_history_view p2
                WHERE p1.customer_id = p2.customer_id
                    AND p2.group_id = p1.group_id
                    AND Group_Summ_Paid != Group_Summ
            ) as group_average_discount
        FROM periods_view p1
        ORDER BY 1,
            2
    )