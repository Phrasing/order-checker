-- Migration 015: Optimize indexes for cursor-based pagination
-- This migration improves query performance for large datasets by creating
-- composite indexes optimized for the pagination queries

-- Drop old inefficient single-column indexes that are superseded by composite indexes
DROP INDEX IF EXISTS idx_orders_date;
DROP INDEX IF EXISTS idx_orders_account_date;

-- Create composite index for cursor pagination on ALL orders
-- Index on (effective_date DESC, id ASC) enables efficient cursor-based pagination
-- effective_date is the COALESCE expression used in ORDER BY
CREATE INDEX IF NOT EXISTS idx_orders_effective_date_id ON orders(
  COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date) DESC,
  id ASC
);

-- Create composite index for cursor pagination filtered by account_id
-- Covers the most common query pattern: filtered by account, ordered by effective_date
CREATE INDEX IF NOT EXISTS idx_orders_account_effective_date_id ON orders(
  account_id,
  COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date) DESC,
  id ASC
);

-- Create partial index for recent orders (most commonly accessed)
-- Only indexes orders from the last 90 days to reduce index size and improve performance
-- This covers the majority of dashboard queries while keeping the index small
CREATE INDEX IF NOT EXISTS idx_orders_recent ON orders(
  account_id,
  COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date) DESC
) WHERE date(COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date)) >= date('now', '-90 days');

-- Keep the status index as it's used for status filtering
-- idx_orders_status already exists from migration 010

-- Keep the account index for account filtering
-- idx_orders_account already exists from migration 010
