-- Add likes and comments columns to project_submission table
ALTER TABLE project_submission ADD COLUMN IF NOT EXISTS likes INTEGER DEFAULT 0;
ALTER TABLE project_submission ADD COLUMN IF NOT EXISTS comments INTEGER DEFAULT 0;

