drop TABLE gcp_nira_demo.Produce;

CREATE TABLE gcp_nira_demo.Produce (
  product STRING,
  inventory INT64)
OPTIONS(enable_change_history=true);

INSERT INTO gcp_nira_demo.Produce
VALUES
  ('bananas', 20),
  ('carrots', 30);

  DELETE gcp_nira_demo.Produce
WHERE product = 'bananas';


UPDATE gcp_nira_demo.Produce
SET inventory = inventory - 10
WHERE product = 'carrots';

CREATE TABLE gcp_nira_demo.Produce (product STRING, inventory INT64) AS (
  SELECT 'apples' AS product, 10 AS inventory);


  SELECT
  product,
  inventory,
  _CHANGE_TYPE AS change_type,
  _CHANGE_TIMESTAMP AS change_time
FROM
  CHANGES(TABLE gcp_nira_demo.Produce, NULL, '2024-07-23 14:19:00')
ORDER BY change_time, product;




 SELECT
  product,
  inventory,
  _CHANGE_TYPE AS change_type,
  _CHANGE_TIMESTAMP AS change_time
FROM
  APPENDS(TABLE gcp_nira_demo.Produce, NULL, NULL);

  ALTER TABLE gcp_nira_demo.Produce ADD COLUMN color STRING;
INSERT INTO gcp_nira_demo.Produce VALUES ('grapes', 40, 'purple');
UPDATE gcp_nira_demo.Produce SET inventory = inventory + 5 WHERE TRUE;
DELETE gcp_nira_demo.Produce WHERE product = 'bananas';

SELECT
  product,
  inventory,
  color,
  _CHANGE_TYPE AS change_type,
  _CHANGE_TIMESTAMP AS change_time
FROM
  APPENDS(TABLE gcp_nira_demo.Produce, NULL, NULL);