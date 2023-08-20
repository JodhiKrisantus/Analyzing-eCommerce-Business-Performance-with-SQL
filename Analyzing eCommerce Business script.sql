--1. CREATE TABLE & ERD

CREATE TABLE IF NOT EXISTS public.customer_dataset
(
    customer_id character varying COLLATE pg_catalog."default" NOT NULL,
    customer_unique_id character varying COLLATE pg_catalog."default",
    customer_zip_code_prefix numeric,
    customer_city character varying COLLATE pg_catalog."default",
    customer_state character varying COLLATE pg_catalog."default",
    CONSTRAINT customer_dataset_pkey PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS public.geolocation_dataset
(
    geolocation_zip_code_prefix integer,
    geolocation_lat numeric,
    geolocation_lng numeric,
    geolocation_city character varying COLLATE pg_catalog."default",
    geolocation_state character varying COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS public.order_dataset
(
    order_id character varying COLLATE pg_catalog."default" NOT NULL,
    customer_id character varying COLLATE pg_catalog."default",
    order_status character varying COLLATE pg_catalog."default",
    order_purchase_timestamp timestamp without time zone,
    order_approved_at timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone,
    CONSTRAINT order_dataset_pkey PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS public.order_items_dataset
(
    order_id character varying COLLATE pg_catalog."default",
    order_item_id integer,
    product_id character varying COLLATE pg_catalog."default",
    seller_id character varying COLLATE pg_catalog."default",
    shipping_limit_date timestamp without time zone,
    price numeric,
    freight_value numeric
);

CREATE TABLE IF NOT EXISTS public.order_payments_dataset
(
    order_id character varying COLLATE pg_catalog."default",
    payment_sequential integer,
    payment_type character varying COLLATE pg_catalog."default",
    payment_installments integer,
    payment_value numeric
);

CREATE TABLE IF NOT EXISTS public.order_reviews_dataset
(
    review_id character varying COLLATE pg_catalog."default",
    order_id character varying COLLATE pg_catalog."default",
    review_score integer,
    review_comment_title character varying COLLATE pg_catalog."default",
    review_comment_message character varying COLLATE pg_catalog."default",
    review_creation_date timestamp without time zone,
    review_answer_timestamp timestamp without time zone
);

CREATE TABLE IF NOT EXISTS public.product_dataset
(
    product_id character varying COLLATE pg_catalog."default" NOT NULL,
    product_category_name character varying COLLATE pg_catalog."default",
    product_name_lenght integer,
    product_description_lenght integer,
    product_photos_qty integer,
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer,
    CONSTRAINT product_dataset_pkey PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS public.sellers_dataset
(
    seller_id character varying COLLATE pg_catalog."default" NOT NULL,
    seller_zip_code_prefix numeric,
    seller_city character varying COLLATE pg_catalog."default",
    seller_state character varying COLLATE pg_catalog."default",
    CONSTRAINT sellers_dataset_pkey PRIMARY KEY (seller_id)
);

ALTER TABLE IF EXISTS public.customer_dataset
    ADD CONSTRAINT customer_zip_code_prefix_fk FOREIGN KEY (customer_zip_code_prefix)
    REFERENCES public.geolocation_dataset (geolocation_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
ALTER TABLE IF EXISTS public.order_dataset
    ADD CONSTRAINT customer_id_fk FOREIGN KEY (customer_id)
    REFERENCES public.customer_dataset (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.order_items_dataset
    ADD CONSTRAINT order_id FOREIGN KEY (order_id)
    REFERENCES public.order_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.order_items_dataset
    ADD CONSTRAINT product_id FOREIGN KEY (product_id)
    REFERENCES public.product_dataset (product_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.order_items_dataset
    ADD CONSTRAINT seller_id FOREIGN KEY (seller_id)
    REFERENCES public.sellers_dataset (seller_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.order_payments_dataset
    ADD CONSTRAINT order_id_fk FOREIGN KEY (order_id)
    REFERENCES public.order_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.order_reviews_dataset
    ADD CONSTRAINT order_id FOREIGN KEY (order_id)
    REFERENCES public.order_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.sellers_dataset
    ADD CONSTRAINT seller_zip_code_prefix_fk FOREIGN KEY (seller_zip_code_prefix)
    REFERENCES public.geolocation_dataset (geolocation_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
END;


--2. Annual Customer Activity Growth SQL
-- total customer baru per tahun
WITH new_customer AS
	(SELECT CD.CUSTOMER_UNIQUE_ID,
			DATE_PART('YEAR',
			MIN(OD.ORDER_PURCHASE_TIMESTAMP)) TAHUN
		FROM ORDER_DATASET AS OD
		JOIN CUSTOMER_DATASET CD ON OD.CUSTOMER_ID = CD.CUSTOMER_ID
		GROUP BY CUSTOMER_UNIQUE_ID), 
-- Rata-rata Monthly Active User (MAU) per tahun
avg_mau_year AS
	(SELECT DATE_PART('YEAR',
			OD.ORDER_PURCHASE_TIMESTAMP) AS TAHUN,
			DATE_PART('MONTH',
			OD.ORDER_PURCHASE_TIMESTAMP) AS MAU,
			COUNT(DISTINCT CD.CUSTOMER_UNIQUE_ID) AS JUMLAH_CUS
		FROM ORDER_DATASET OD
		JOIN CUSTOMER_DATASET CD ON OD.CUSTOMER_ID = CD.CUSTOMER_ID
		GROUP BY TAHUN,
			MAU
		ORDER BY TAHUN ASC,MAU ASC), 
-- Rata-Rata frekuensi order untuk setiap tahun
-- Menampilkan Rata rata jumlah order yang dilakukan customer untuk masing masing tahun
order_frequency AS
	(SELECT DATE_PART('YEAR',
			OD.ORDER_PURCHASE_TIMESTAMP) AS TAHUN,
			CS.CUSTOMER_UNIQUE_ID,
			COUNT(OD.ORDER_ID) AS JUMLAH_ORDER_CUSTOMER
		FROM ORDER_DATASET OD
		JOIN CUSTOMER_DATASET CS ON OD.CUSTOMER_ID = CS.CUSTOMER_ID
		GROUP BY CS.CUSTOMER_UNIQUE_ID,
TAHUN), 
-- jumlah customer yang melakukan repeat order per tahun
-- Menampilkan jumlah customer yang melakukan pembelian lebih dari satu kali.
-- (repeat order) pada masing-masing tahun
customer_repeat_order AS
	(SELECT DATE_PART('YEAR',
			OD.ORDER_PURCHASE_TIMESTAMP) AS TAHUN,
			CD.CUSTOMER_UNIQUE_ID,
			COUNT(OD.ORDER_ID) AS TOTAL_ORDER
		FROM ORDER_DATASET OD
		JOIN CUSTOMER_DATASET CD ON CD.CUSTOMER_ID = OD.CUSTOMER_ID
		GROUP BY CUSTOMER_UNIQUE_ID,
			TAHUN
		HAVING COUNT(OD.ORDER_ID) > 1)
-- Gabungkan CTE
SELECT B1.TAHUN,
	COUNT(B1.CUSTOMER_UNIQUE_ID) AS NEW_CUSTOMER,
	(SELECT floor (AVG(J2.JUMLAH_CUS)) AS MAU_YEAR
		FROM avg_mau_year J2
		WHERE B1.TAHUN = J2.TAHUN),
	(SELECT AVG(JUMLAH_ORDER_CUSTOMER) AS FREKUENSI_ORDER
		FROM order_frequency
		WHERE B1.TAHUN = order_frequency.TAHUN),
	(SELECT COUNT(CUSTOMER_UNIQUE_ID) AS REPEAT_ORDER
		FROM customer_repeat_order
		WHERE B1.TAHUN = customer_repeat_order.TAHUN)
FROM new_customer B1
GROUP BY B1.TAHUN
ORDER BY B1.TAHUN


--3. Annual Product Category Quality Analysis SQL
-- Membuat tabel yang berisi informasi pendapatan/revenue perusahaan total untuk masing-masing tahun
-- (Hint: Revenue adalah harga barang dan juga biaya kirim.
-- Pastikan juga melakukan filtering terhadap order status yang tepat untuk menghitung pendapatan)
WITH TABLE_REVENUE AS (
  SELECT 
    DATE_PART(
      'YEAR', OD.ORDER_PURCHASE_TIMESTAMP
    ) AS TAHUN, 
    SUM(OID.PRICE + OID.FREIGHT_VALUE) AS REVENUE 
  FROM 
    ORDER_DATASET AS OD 
    JOIN ORDER_ITEMS_DATASET AS OID ON OD.ORDER_ID = OID.ORDER_ID 
  WHERE 
    OD.ORDER_STATUS = 'approved' 
    OR OD.ORDER_STATUS = 'delivered' 
  GROUP BY 
    TAHUN 
  ORDER BY 
    TAHUN
), 
-- Membuat tabel yang berisi informasi jumlah cancel order total untuk masing-masing tahun
TABLE_CANCEL AS (
  SELECT 
    DATE_PART(
      'YEAR', ORDER_PURCHASE_TIMESTAMP
    ) AS TAHUN, 
    COUNT(ORDER_STATUS) AS TOTAL_CANCELED 
  FROM 
    ORDER_DATASET 
  WHERE 
    ORDER_STATUS = 'canceled' 
  GROUP BY 
    TAHUN 
  ORDER BY 
    TAHUN
), 
-- Membuat tabel yang berisi nama kategori produk yang memberikan pendapatan total tertinggi untuk masing-masing tahun
REVENUE_CATEGORY_TABLE AS (
  SELECT 
    REVENUE_PRODUCT.TAHUN, 
    PD.PRODUCT_CATEGORY_NAME, 
    SUM(REVENUE_PRODUCT.REVENUE) AS REVENUE, 
    RANK() OVER (
      PARTITION BY REVENUE_PRODUCT.TAHUN 
      ORDER BY 
        SUM(REVENUE_PRODUCT.REVENUE) DESC
    ) AS RANKING 
  FROM 
    (
      SELECT 
        DATE_PART(
          'YEAR', OD.ORDER_PURCHASE_TIMESTAMP
        ) AS TAHUN, 
        OID.PRODUCT_ID, 
        OID.PRICE + OID.FREIGHT_VALUE AS REVENUE 
      FROM 
        ORDER_DATASET AS OD 
        JOIN ORDER_ITEMS_DATASET AS OID ON OD.ORDER_ID = OID.ORDER_ID 
      WHERE 
        OD.ORDER_STATUS = 'approved' 
        OR OD.ORDER_STATUS = 'delivered'
    ) AS REVENUE_PRODUCT 
    JOIN PRODUCT_DATASET AS PD ON PD.PRODUCT_ID = REVENUE_PRODUCT.PRODUCT_ID 
  GROUP BY 
    REVENUE_PRODUCT.TAHUN, 
    PD.PRODUCT_CATEGORY_NAME
), 
-- Membuat tabel yang berisi nama kategori produk yang memiliki jumlah cancel order terbanyak untuk masing-masing tahun
CANCEL_CATEGORY_TABLE AS (
  SELECT 
    REVENUE_PRODUCT.TAHUN, 
    PD.PRODUCT_CATEGORY_NAME, 
    COUNT(PD.PRODUCT_CATEGORY_NAME) AS JUMLAH_CANCELED, 
    RANK() OVER(
      PARTITION BY TAHUN 
      ORDER BY 
        COUNT(PD.PRODUCT_CATEGORY_NAME) DESC
    ) AS RANKING 
  FROM 
    (
      SELECT 
        DATE_PART(
          'YEAR', OD.ORDER_PURCHASE_TIMESTAMP
        ) AS TAHUN, 
        OID.PRODUCT_ID 
      FROM 
        ORDER_DATASET AS OD 
        JOIN ORDER_ITEMS_DATASET AS OID ON OD.ORDER_ID = OID.ORDER_ID 
      WHERE 
        OD.ORDER_STATUS = 'canceled'
    ) AS REVENUE_PRODUCT 
    JOIN PRODUCT_DATASET AS PD ON PD.PRODUCT_ID = REVENUE_PRODUCT.PRODUCT_ID 
  GROUP BY 
    REVENUE_PRODUCT.TAHUN, 
    PD.PRODUCT_CATEGORY_NAME 
  ORDER BY 
    TAHUN, 
    JUMLAH_CANCELED DESC
) 
-- Menggabungkan informasi-informasi yang telah didapatkan ke dalam satu tampilan tabel
SELECT 
  TABLE_REVENUE.TAHUN, 
  TABLE_REVENUE.REVENUE, 
  (
    SELECT 
      TOTAL_CANCELED 
    FROM 
      TABLE_CANCEL 
    WHERE 
      TABLE_CANCEL.TAHUN = TABLE_REVENUE.TAHUN
  ), 
  (
    SELECT 
      PRODUCT_CATEGORY_NAME as most_revenue_product
    FROM 
      REVENUE_CATEGORY_TABLE 
    WHERE 
      REVENUE_CATEGORY_TABLE.TAHUN = TABLE_REVENUE.TAHUN 
      AND RANKING = 1
  ), 
  (
    SELECT 
      PRODUCT_CATEGORY_NAME as most_canceled
    FROM 
      CANCEL_CATEGORY_TABLE 
    WHERE 
      CANCEL_CATEGORY_TABLE.TAHUN = TABLE_REVENUE.TAHUN 
      AND RANKING = 1
  ) 
FROM 
  TABLE_REVENUE


--4. Analysis Of Annual Payment Type Usage
-- Menampilkan jumlah penggunaan masing-masing tipe pembayaran secara all time diurutkan dari yang terfavorit 

select 
	DATE_PART('YEAR',od.order_purchase_timestamp) as tahun,
	opd.payment_type,
	count(od.order_id) jumlah_penggunaan,
	floor (avg(opd.payment_installments)) avg_lama_angsurang
from order_dataset as od
join order_payments_dataset as opd
on od.order_id = opd.order_id 
where od.order_status != 'canceled' and  od.order_status != 'unavailable'
group by tahun,payment_type
order by tahun, jumlah_penggunaan desc