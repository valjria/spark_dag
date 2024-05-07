DROP TABLE public.clean_trans_limited;
CREATE TABLE IF NOT EXISTS public.clean_trans_limited (
    "PRODUCT_CATEGORY" text,
    "PRODUCT_ID" text,
    "MRP" real,
    "CP" real,
    "DISCOUNT" real,
    "SP" real,
    sales_date date
);
