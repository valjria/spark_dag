INSERT INTO public.clean_trans_limited
SELECT "PRODUCT_CATEGORY", "PRODUCT_ID", "MRP","CP", "DISCOUNT", "SP", "sales_date"
FROM clean_transactions;