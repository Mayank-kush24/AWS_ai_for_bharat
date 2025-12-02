-- Migration script to fix kiro_submission logging trigger
-- This fixes the issue where week_number (INTEGER) needs to be cast to TEXT for concatenation

-- Update the log_activity function to properly handle kiro_submission
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
    ELSIF TG_TABLE_NAME = 'kiro_submission' THEN
        record_id := COALESCE(NEW.week_number::TEXT, 'NULL') || '|' || COALESCE(NEW.email, 'NULL');
    ELSE
        -- Fallback for unknown tables
        record_id := COALESCE(NEW.email, 'unknown') || '|' || TG_TABLE_NAME;
    END IF;

    -- Safety check: ensure record_id is not null
    IF record_id IS NULL OR record_id = '' THEN
        record_id := TG_TABLE_NAME || '|' || 'unknown';
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
        ELSIF TG_TABLE_NAME = 'kiro_submission' THEN
            record_id := COALESCE(OLD.week_number::TEXT, 'NULL') || '|' || COALESCE(OLD.email, 'NULL');
        ELSE
            record_id := COALESCE(OLD.email, 'unknown') || '|' || TG_TABLE_NAME;
        END IF;
        
        -- Safety check: ensure record_id is not null
        IF record_id IS NULL OR record_id = '' THEN
            record_id := TG_TABLE_NAME || '|' || 'unknown';
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

