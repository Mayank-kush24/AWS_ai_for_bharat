-- Add validation_reason column to project_submission table
ALTER TABLE project_submission ADD COLUMN IF NOT EXISTS validation_reason VARCHAR(255);
