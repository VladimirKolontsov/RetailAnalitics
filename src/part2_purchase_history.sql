DROP MATERIALIZED VIEW IF EXISTS Purchase_history_View CASCADE;
--------------------------- Drop materialized view: Purchase_history_View ---------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS Purchase_history_View AS
SELECT
    c.customer_id,
    t.transaction_id,
    t.transaction_date_time,
    gr.group_id,
    SUM(
        st.sku_purchase_price * ch.sku_amount
    ) AS group_cost,
    SUM(ch.sku_summ) AS group_summ,
    SUM(ch.sku_summ_paid) AS group_summ_paid
FROM
    customer_cards cc
    JOIN transactions t USING (customer_card_id)
    JOIN checks ch USING (transaction_id)
    JOIN sales_stores st USING (sku_id, transaction_store_id)
    JOIN product_matrix pm USING (sku_id)
    JOIN sku_groups gr USING (group_id)
    RIGHT JOIN customers c USING (customer_id)
WHERE
    t.transaction_date_time <= (
        SELECT MAX(analysis_formation)
        FROM date_of_analysis_formation
    )
    or t.transaction_date_time IS NULL
GROUP BY
    c.customer_id,
    t.transaction_id,
    gr.group_id
ORDER BY c.customer_id;
----------------------------- Create materialized view:  Purchase_history_View-----------------------------------------
SELECT * FROM Purchase_history_View;
----------------------------- Test -----------------------------