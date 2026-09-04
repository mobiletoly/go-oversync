-- The caller proves generation retirement, a retired catalog identity, empty
-- retained history and row state, and the closed writer fence in this transaction.
-- RESTRICT intentionally makes any unaccounted dependency fail atomically.
DROP TABLE main.legacy_notes RESTRICT;
