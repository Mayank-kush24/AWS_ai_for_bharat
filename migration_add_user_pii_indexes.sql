-- Migration: Add indexes to user_pii table for faster search and queries
-- This improves performance for the users list page and search functionality

-- Index on name for faster name-based searches
CREATE INDEX IF NOT EXISTS idx_user_pii_name ON user_pii(name);

-- Index on phone_number for faster phone-based searches
CREATE INDEX IF NOT EXISTS idx_user_pii_phone_number ON user_pii(phone_number);

-- Index on country for faster country-based searches
CREATE INDEX IF NOT EXISTS idx_user_pii_country ON user_pii(country);

-- Index on created_at for faster sorting (already used in ORDER BY)
CREATE INDEX IF NOT EXISTS idx_user_pii_created_at ON user_pii(created_at DESC);

-- Composite index for common search patterns (name, email, country)
CREATE INDEX IF NOT EXISTS idx_user_pii_search ON user_pii(name, email, country);

