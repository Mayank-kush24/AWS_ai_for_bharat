-- Migration: Add user_edit_any permission
-- This permission controls whether users can edit any user profile from the user section

INSERT INTO rbac_permissions (route_name, display_name, description, category)
VALUES ('user_edit_any', 'Edit Any User Profile', 'Allow editing any user profile from the user section', 'Users')
ON CONFLICT (route_name) DO NOTHING;

