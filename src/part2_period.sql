DROP MATERIALIZED VIEW IF EXISTS Period_View CASCADE;
--------------------------- Drop materialized view: Period_View ---------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS Period_View AS
WITH
    base_info AS (
        SELECT
            cc.customer_id,
            gr.group_id,
            transaction_id,
            transaction_date_time,
            sku_discount / sku_summ AS discount
        FROM
            customer_cards cc
            JOIN transactions t USING (customer_card_id)
            JOIN checks ch USING (transaction_id)
            JOIN sales_stores st USING (sku_id, transaction_store_id)
            JOIN product_matrix pm USING (sku_id)
            JOIN sku_groups gr USING (group_id)
        WHERE
            t.transaction_date_time <= (
                SELECT MAX(analysis_formation)
                FROM date_of_analysis_formation
            )
    ),
    group_info AS (
        SELECT
            customer_id,
            group_id,
            MIN(transaction_date_time) AS First_Group_Purchase_Date,
            MAX(transaction_date_time) AS Last_Group_Purchase_Date,
            COUNT(*) AS Group_Purchase,
            CASE
                WHEN MAX(discount) = 0 THEN 0
                ELSE (
                    MIN(discount) FILTER (
                        WHERE
                            discount <> 0
                    )
                )
            END AS Group_Min_Discount
        FROM base_info
        GROUP BY
            customer_id,
            group_id
    )
SELECT
    c.customer_id,
    g.group_id,
    g.First_Group_Purchase_Date,
    g.Last_Group_Purchase_Date,
    g.Group_Purchase,
    (
        EXTRACT(
            EPOCH
            FROM (
                    g.Last_Group_Purchase_Date - g.First_Group_Purchase_Date
                )
        ) + 86400
    ) / (86400 * g.Group_Purchase) AS Group_Frequency,
    g.Group_Min_Discount
FROM group_info g
    RIGHT JOIN customers c USING (customer_id)
GROUP BY
    c.customer_id,
    g.group_id,
    g.First_Group_Purchase_Date,
    g.Last_Group_Purchase_Date,
    g.Group_Purchase,
    g.Group_Min_Discount
ORDER BY c.customer_id;
----------------------------- Create materialized view:Period_View-----------------------------------------

SELECT * FROM Period_View;
----------------------------- Test -----------------------------