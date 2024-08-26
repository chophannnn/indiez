CREATE DATABASE interview;

USE interview;

CREATE TABLE tree (
  id          INT,
  parent_id   INT
);

INSERT INTO tree
VALUES
  (1, 4),
  (2, 4),
  (3, 7),
  (4, 5),
  (5, 7),
  (6, 5),
  (7, NULL),
  (8, 7),
  (9, 8),
  (10, 9),
  (11, 8),
  (12, 6),
  (13, 12),
  (14, 12),
  (15, 2);
