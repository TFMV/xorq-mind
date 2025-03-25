SELECT
  *
FROM (
  SELECT
    "t1"."region",
    "t1"."date",
    SUM("t1"."total_price") AS "daily_sales",
    AVG("t1"."total_price") AS "avg_order_value",
    COUNT(*) AS "num_transactions"
  FROM (
    SELECT
      *
    FROM "ibis_read_parquet_bkceovroh5cahneab5mm6gzs7u" AS "t0"
    WHERE
      "t0"."unit_price" > 100
  ) AS "t1"
  GROUP BY
    1,
    2
) AS "t2"
ORDER BY
  "t2"."region" ASC,
  "t2"."date" ASC