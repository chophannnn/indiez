SELECT
  id,
  parent_id,
  CASE
    WHEN parent_id IS NULL THEN "root"
    WHEN id IN (SELECT DISTINCT parent_id FROM tree WHERE parent_id IS NOT NULL) THEN "inner"
    ELSE "leaf"
  END AS type
FROM tree;
