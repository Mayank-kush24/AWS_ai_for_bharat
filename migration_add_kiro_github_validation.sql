-- Migration script to add GitHub validation columns to kiro_submission table

-- Add GitHub validation columns if they don't exist
ALTER TABLE kiro_submission ADD COLUMN IF NOT EXISTS github_valid BOOLEAN DEFAULT FALSE;
ALTER TABLE kiro_submission ADD COLUMN IF NOT EXISTS github_validation_reason VARCHAR(255);

