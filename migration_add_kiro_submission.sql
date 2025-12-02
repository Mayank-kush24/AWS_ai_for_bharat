-- Migration script to add kiro_submission table
-- Run this to add support for Kiro Challenge submissions

-- ============================================
-- Table: Kiro Submission
-- ============================================
CREATE TABLE IF NOT EXISTS kiro_submission (
    week_number INTEGER NOT NULL,
    email VARCHAR(255) NOT NULL,
    github_link TEXT,
    blog_link TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (week_number, email),
    FOREIGN KEY (email) REFERENCES user_pii(email) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Create index on week_number for faster queries
CREATE INDEX IF NOT EXISTS idx_kiro_submission_week_number ON kiro_submission(week_number);
CREATE INDEX IF NOT EXISTS idx_kiro_submission_email ON kiro_submission(email);

-- Apply updated_at trigger
CREATE TRIGGER update_kiro_submission_updated_at BEFORE UPDATE ON kiro_submission
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add logging trigger
CREATE TRIGGER log_kiro_submission_activity
    AFTER INSERT OR UPDATE OR DELETE ON kiro_submission
    FOR EACH ROW EXECUTE FUNCTION log_activity();

