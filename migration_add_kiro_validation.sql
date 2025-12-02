-- Migration script to add validation columns to kiro_submission table
-- Adds support for blog validation, likes, and comments tracking

-- Add validation columns if they don't exist
ALTER TABLE kiro_submission ADD COLUMN IF NOT EXISTS valid BOOLEAN DEFAULT FALSE;
ALTER TABLE kiro_submission ADD COLUMN IF NOT EXISTS validation_reason VARCHAR(255);
ALTER TABLE kiro_submission ADD COLUMN IF NOT EXISTS likes INTEGER DEFAULT 0;
ALTER TABLE kiro_submission ADD COLUMN IF NOT EXISTS comments INTEGER DEFAULT 0;

