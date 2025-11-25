-- AWS AI for Bharat Tracking System Database Schema
-- This file creates all tables with proper primary keys and foreign keys

-- ============================================
-- Table 1: User PII (Personal Identifiable Information)
-- ============================================
CREATE TABLE IF NOT EXISTS user_pii (
    email VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    registration_date_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    phone_number VARCHAR(20),
    gender VARCHAR(20),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    date_of_birth DATE,
    designation VARCHAR(255),
    class_stream VARCHAR(255),
    degree_passout_year INTEGER,
    occupation VARCHAR(255),
    linkedin VARCHAR(500),
    participated_in_academy_1_0 BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Table 2: Form Response
-- ============================================
CREATE TABLE IF NOT EXISTS form_response (
    email VARCHAR(255),
    form_name VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    time_slot TIMESTAMP,
    time_slot_range VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (email, form_name),
    FOREIGN KEY (email) REFERENCES user_pii(email) ON DELETE CASCADE ON UPDATE CASCADE
);

-- ============================================
-- Table 3: AWS Team Building
-- ============================================
CREATE TABLE IF NOT EXISTS aws_team_building (
    workshop_name VARCHAR(255),
    email VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    workshop_link VARCHAR(500),
    team_id VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workshop_name, email),
    FOREIGN KEY (email) REFERENCES user_pii(email) ON DELETE CASCADE ON UPDATE CASCADE
);

-- ============================================
-- Table 4: Project Submission
-- ============================================
CREATE TABLE IF NOT EXISTS project_submission (
    workshop_name VARCHAR(255),
    email VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    project_link VARCHAR(500),
    valid BOOLEAN DEFAULT FALSE,
    team_id VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workshop_name, email),
    FOREIGN KEY (email) REFERENCES user_pii(email) ON DELETE CASCADE ON UPDATE CASCADE
);

-- ============================================
-- Table 5: Verification
-- ============================================
CREATE TABLE IF NOT EXISTS verification (
    workshop_name VARCHAR(255),
    email VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    project_ss VARCHAR(500),
    project_valid BOOLEAN DEFAULT FALSE,
    blog VARCHAR(500),
    blog_valid BOOLEAN DEFAULT FALSE,
    team_id VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workshop_name, email),
    FOREIGN KEY (email) REFERENCES user_pii(email) ON DELETE CASCADE ON UPDATE CASCADE
);

-- ============================================
-- Table 6: Master Logs (Automatic Activity Tracking)
-- ============================================
CREATE TABLE IF NOT EXISTS master_logs (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation_type VARCHAR(20) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    record_identifier TEXT NOT NULL, -- Composite key or primary key value
    old_values JSONB, -- Previous values (for UPDATE/DELETE)
    new_values JSONB, -- New values (for INSERT/UPDATE)
    changed_by VARCHAR(255), -- User/system that made the change
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    additional_info JSONB -- For any extra metadata
);

-- Create index on table_name and timestamp for faster queries
CREATE INDEX IF NOT EXISTS idx_master_logs_table_name ON master_logs(table_name);
CREATE INDEX IF NOT EXISTS idx_master_logs_timestamp ON master_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_master_logs_operation ON master_logs(operation_type);

-- ============================================
-- Function: Update updated_at timestamp
-- ============================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply updated_at trigger to all tables
CREATE TRIGGER update_user_pii_updated_at BEFORE UPDATE ON user_pii
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_form_response_updated_at BEFORE UPDATE ON form_response
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_aws_team_building_updated_at BEFORE UPDATE ON aws_team_building
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_project_submission_updated_at BEFORE UPDATE ON project_submission
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_verification_updated_at BEFORE UPDATE ON verification
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- Function: Log activity to master_logs
-- ============================================
CREATE OR REPLACE FUNCTION log_activity()
RETURNS TRIGGER AS $$
DECLARE
    record_id TEXT;
    old_data JSONB;
    new_data JSONB;
BEGIN
    -- Determine record identifier based on table
    IF TG_TABLE_NAME = 'user_pii' THEN
        record_id := NEW.email;
    ELSIF TG_TABLE_NAME = 'form_response' THEN
        record_id := NEW.email || '|' || NEW.form_name;
    ELSIF TG_TABLE_NAME = 'aws_team_building' THEN
        record_id := NEW.workshop_name || '|' || NEW.email;
    ELSIF TG_TABLE_NAME = 'project_submission' THEN
        record_id := NEW.workshop_name || '|' || NEW.email;
    ELSIF TG_TABLE_NAME = 'verification' THEN
        record_id := NEW.workshop_name || '|' || NEW.email;
    END IF;

    -- Handle INSERT operation
    IF TG_OP = 'INSERT' THEN
        new_data := to_jsonb(NEW);
        INSERT INTO master_logs (
            table_name,
            operation_type,
            record_identifier,
            new_values,
            timestamp
        ) VALUES (
            TG_TABLE_NAME,
            'INSERT',
            record_id,
            new_data,
            CURRENT_TIMESTAMP
        );
        RETURN NEW;
    END IF;

    -- Handle UPDATE operation
    IF TG_OP = 'UPDATE' THEN
        old_data := to_jsonb(OLD);
        new_data := to_jsonb(NEW);
        INSERT INTO master_logs (
            table_name,
            operation_type,
            record_identifier,
            old_values,
            new_values,
            timestamp
        ) VALUES (
            TG_TABLE_NAME,
            'UPDATE',
            record_id,
            old_data,
            new_data,
            CURRENT_TIMESTAMP
        );
        RETURN NEW;
    END IF;

    -- Handle DELETE operation
    IF TG_OP = 'DELETE' THEN
        old_data := to_jsonb(OLD);
        -- Determine record identifier for DELETE
        IF TG_TABLE_NAME = 'user_pii' THEN
            record_id := OLD.email;
        ELSIF TG_TABLE_NAME = 'form_response' THEN
            record_id := OLD.email || '|' || OLD.form_name;
        ELSIF TG_TABLE_NAME = 'aws_team_building' THEN
            record_id := OLD.workshop_name || '|' || OLD.email;
        ELSIF TG_TABLE_NAME = 'project_submission' THEN
            record_id := OLD.workshop_name || '|' || OLD.email;
        ELSIF TG_TABLE_NAME = 'verification' THEN
            record_id := OLD.workshop_name || '|' || OLD.email;
        END IF;
        
        INSERT INTO master_logs (
            table_name,
            operation_type,
            record_identifier,
            old_values,
            timestamp
        ) VALUES (
            TG_TABLE_NAME,
            'DELETE',
            record_id,
            old_data,
            CURRENT_TIMESTAMP
        );
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- Triggers: Automatic logging for all tables
-- ============================================
CREATE TRIGGER log_user_pii_activity
    AFTER INSERT OR UPDATE OR DELETE ON user_pii
    FOR EACH ROW EXECUTE FUNCTION log_activity();

CREATE TRIGGER log_form_response_activity
    AFTER INSERT OR UPDATE OR DELETE ON form_response
    FOR EACH ROW EXECUTE FUNCTION log_activity();

CREATE TRIGGER log_aws_team_building_activity
    AFTER INSERT OR UPDATE OR DELETE ON aws_team_building
    FOR EACH ROW EXECUTE FUNCTION log_activity();

CREATE TRIGGER log_project_submission_activity
    AFTER INSERT OR UPDATE OR DELETE ON project_submission
    FOR EACH ROW EXECUTE FUNCTION log_activity();

CREATE TRIGGER log_verification_activity
    AFTER INSERT OR UPDATE OR DELETE ON verification
    FOR EACH ROW EXECUTE FUNCTION log_activity();

