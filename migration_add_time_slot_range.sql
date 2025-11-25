-- Migration script to add time_slot_range column to form_response table
-- Run this to add support for storing the full time range string

-- Add time_slot_range column if it doesn't exist
ALTER TABLE form_response ADD COLUMN IF NOT EXISTS time_slot_range VARCHAR(255);

