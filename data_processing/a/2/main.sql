PREPARE PREP_DESCENDENTS FROM "
  WITH RECURSIVE descendents(id, parent_id) AS (
    SELECT
      id,
      parent_id
    FROM tree
    WHERE id = ?
    UNION ALL
    SELECT
      tree.id,
      tree.parent_id
    FROM
      tree
      INNER JOIN descendents ON tree.parent_id=descendents.id
  )
  SELECT CONCAT((SELECT id FROM descendents LIMIT 1), ': ', GROUP_CONCAT(id SEPARATOR ', ')) AS id
  FROM descendents
";

SET @id = 5;

EXECUTE PREP_DESCENDENTS USING @id;
