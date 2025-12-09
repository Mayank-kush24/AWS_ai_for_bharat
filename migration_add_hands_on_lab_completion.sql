-- Migration: Add Hands-on Lab Completion Proof table
-- This table stores hands-on lab completion proofs with workshop information

CREATE TABLE IF NOT EXISTS hands_on_lab_completion (
    workshop_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    problem_statement VARCHAR(500),
    hands_on_lab_proof_link VARCHAR(500),
    valid BOOLEAN DEFAULT FALSE,
    assigned_to VARCHAR(255),
    assigned_at TIMESTAMP,
    blog_submission VARCHAR(500),
    remarks TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workshop_name, email),
    FOREIGN KEY (email) REFERENCES user_pii(email) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Create index on workshop_name for faster queries
CREATE INDEX IF NOT EXISTS idx_hands_on_lab_completion_workshop_name ON hands_on_lab_completion(workshop_name);
CREATE INDEX IF NOT EXISTS idx_hands_on_lab_completion_email ON hands_on_lab_completion(email);

-- Apply updated_at trigger
CREATE TRIGGER update_hands_on_lab_completion_updated_at BEFORE UPDATE ON hands_on_lab_completion
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add logging trigger
CREATE TRIGGER log_hands_on_lab_completion_activity
    AFTER INSERT OR UPDATE OR DELETE ON hands_on_lab_completion
    FOR EACH ROW EXECUTE FUNCTION log_activity();

