----
-- offer_discount_depth FLOAT
-- DROP FUNCTION fnc_increase_visits_frequency;
CREATE OR REPLACE FUNCTION fnc_increase_visits_frequency (
    in_first_date TIMESTAMP,
    in_last_date TIMESTAMP,
    in_count BIGINT,
    in_max_churn BIGINT,
    in_max_num_with_discount BIGINT,
    in_margin INT
  ) RETURNS TABLE (
    customer_id BIGINT,
    start_date1 TIMESTAMP,
    end_date TIMESTAMP,
    required_transactions_count BIGINT,
    group_name VARCHAR(100),
    Offer_Discount_Depth BIGINT
  ) AS $$
DECLARE delta FLOAT := EXTRACT(
    DAY
    FROM in_last_date - in_first_date
  );
BEGIN RETURN QUERY WITH CTE_tmp AS (
  SELECT g1.customer_id, s.group_name::VARCHAR(100),
    CEIL(group_minimum_discount / 5.0) * 5.0 as discount,
    group_affinity_index
  FROM groups_view g1
    JOIN sku_group s ON s.group_id = g1.group_id
  WHERE group_churn_rate <= in_max_churn
    AND group_discount_share < in_max_num_with_discount
    AND CEIL(group_minimum_discount / 5.0) * 5.0 <= in_margin * group_margin / 100
  ORDER BY group_affinity_index DESC
) -- -- --
--
--
--
SELECT c1.customer_id::BIGINT,
in_first_date as start_date1,
in_last_date as end_date,
ROUND((delta / c1.customer_frequency)::NUMERIC, 0)::BIGINT + in_count as required_transactions_count,
(
  SELECT tmp.group_name::VARCHAR(100)
  FROM CTE_tmp tmp
  WHERE tmp.customer_id = c1.customer_id
  ORDER BY tmp.group_affinity_index DESC
  LIMIT 1
) AS group_name,
(
  SELECT discount::BIGINT
  FROM CTE_tmp tmp
  WHERE tmp.customer_id = c1.customer_id
  ORDER BY tmp.group_affinity_index DESC
  LIMIT 1
) AS Offer_Discount_Depth
FROM customers_view c1;
END $$ LANGUAGE plpgsql;
--
SELECT *
FROM fnc_increase_visits_frequency(
    '2022-08-18 00:00:00',
    '2022-08-18 00:00:00',
    1,
    3,
    70,
    30
  );
--
  --
--
-- SELECT * FROM (
-- SELECT customer_id,
--   group_name,
--   CEIL(group_minimum_discount / 5.0) * 5.0 as discount,
--   30 * group_margin / 100 as aaa,
--   group_affinity_index
-- FROM groups_view g1
--   JOIN sku_group s ON s.group_id = g1.group_id
-- WHERE group_churn_rate <= 3
--   AND group_discount_share < 70
-- ORDER BY 1,
--   group_affinity_index DESC
-- ) subquery
-- WHERE discount <= aaa
-- ORDER BY 1,
--   group_affinity_index DESC