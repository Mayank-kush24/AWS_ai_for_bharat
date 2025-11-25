-- Migration script to update form_response table primary key
-- Run this if the table already exists with the old structure

-- Drop existing primary key constraint if it exists
ALTER TABLE form_response DROP CONSTRAINT IF EXISTS form_response_pkey;

-- Drop the pk column if it exists
ALTER TABLE form_response DROP COLUMN IF EXISTS pk;

-- Add new composite primary key
ALTER TABLE form_response ADD PRIMARY KEY (email, form_name);

-- Update the trigger function to use form_name instead of pk
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

