SELECT
  *
FROM (
  SELECT
    "t1"."region",
    SUM("t1"."total_price") AS "total_sales",
    AVG("t1"."unit_price") AS "avg_price"
  FROM (
    SELECT
      "t1"."date",
      "t1"."product_id",
      "t1"."customer_id",
      "t1"."quantity",
      "t1"."unit_price",
      "t1"."total_price",
      "t1"."region"
    FROM (
      SELECT
        *
      FROM "sales_data-fddeedf501a1d00a36a75f81059d066b" AS "t0"
      WHERE
        "t0"."unit_price" > 100
    ) AS "t1"
  ) AS t1
  GROUP BY
    "t1"."region"
) AS "t2"
ORDER BY
  "t2"."total_sales" DESC NULLS LAST