# RBAC (Role-Based Access Control) System Setup Guide

This guide will help you set up the RBAC system for the AWS AI for Bharat Tracking System.

## Overview

The RBAC system allows you to:
- Create custom users with login credentials
- Assign specific page permissions to each user
- Admin users have full access to all pages
- Regular users only see and can access pages they have permission for

## Setup Steps

### 1. Install Dependencies

Make sure you have `bcrypt` installed:

```bash
pip install bcrypt==4.1.2
```

Or install all requirements:

```bash
pip install -r requirements.txt
```

### 2. Run Database Migration

Run the RBAC schema migration to create the necessary tables:

```bash
psql -U postgres -d aws_ai_bharat -f migration_rbac_schema.sql
```

This will create:
- `rbac_users` - User accounts for authentication
- `rbac_permissions` - List of all available pages/routes
- `rbac_user_permissions` - Junction table mapping users to permissions

### 3. Create Default Admin User

Run the setup script to create the default admin user:

```bash
python setup_rbac.py
```

This will:
- Create an admin user with username `admin`
- Prompt you to set a password (default: `admin123`)
- Grant all permissions to the admin user

**IMPORTANT**: Change the default password after first login!

### 4. Access the System

1. Start the Flask application:
   ```bash
   python app_web.py
   ```

2. Navigate to: `http://localhost:4000`

3. Login with:
   - Username: `admin`
   - Password: `admin123` (or the password you set)

## Using the RBAC System

### Admin Features

Once logged in as admin, you'll see a "User Management" link in the navigation menu.

#### Creating Users

1. Go to **User Management** → **Create User**
2. Fill in:
   - Username (unique)
   - Email (unique)
   - Full Name (optional)
   - Password
   - Check "Administrator" if you want full access
   - Check "Active" to allow login
3. Click "Create User"

#### Managing Permissions

1. Go to **User Management**
2. Click the key icon (🔑) next to a user
3. Select which pages the user should have access to
4. Click "Save Permissions"

Available permissions:
- **Dashboard** - View dashboard with statistics
- **Users List** - View list of all users
- **Create User** - Create new user records
- **Workshops** - View workshop data and time slots
- **Team Building** - View/create team building records
- **Blog Submissions** - View/create blog submission records
- **Activity Logs** - View activity logs
- **Import Data** - Import data from Excel files

### User Experience

- **Admin users**: See all pages in navigation and have full access
- **Regular users**: Only see pages they have permission for in navigation
- **Unauthorized access**: Users trying to access pages without permission will see an error message

## Security Notes

1. **Change Default Password**: Always change the default admin password after first login
2. **Strong Passwords**: Use strong passwords for all users
3. **Regular Audits**: Periodically review user permissions
4. **Deactivate Unused Accounts**: Set inactive users to "Inactive" status instead of deleting

## Troubleshooting

### Can't Login

- Verify the user exists: Check `rbac_users` table
- Verify user is active: Check `is_active` field
- Reset password: Use the admin interface to update user password

### User Can't See Pages

- Check user permissions: Go to User Management → Permissions
- Verify route names match: Check `rbac_permissions` table
- Clear session: User may need to logout and login again

### Permission Not Working

- Verify permission exists: Check `rbac_permissions` table
- Check user has permission: Check `rbac_user_permissions` table
- Verify route name matches: The route name in decorator must match permission `route_name`

## Database Schema

### rbac_users
- `user_id` - Primary key
- `username` - Unique username
- `email` - Unique email
- `password_hash` - Bcrypt hashed password
- `full_name` - Display name
- `is_admin` - Boolean, admin has all permissions
- `is_active` - Boolean, inactive users can't login
- `created_at`, `updated_at`, `last_login` - Timestamps

### rbac_permissions
- `permission_id` - Primary key
- `route_name` - Unique route name (must match Flask route function name)
- `display_name` - Human-readable name
- `description` - Permission description
- `category` - Grouping category

### rbac_user_permissions
- `user_id` - Foreign key to rbac_users
- `permission_id` - Foreign key to rbac_permissions
- `granted_at` - When permission was granted
- `granted_by` - Which admin granted the permission

## Adding New Permissions

To add a new page permission:

1. Add permission to database:
   ```sql
   INSERT INTO rbac_permissions (route_name, display_name, description, category)
   VALUES ('new_route_name', 'New Page', 'Description of new page', 'Category');
   ```

2. Add permission check to route:
   ```python
   @app.route('/new-page')
   @login_required
   @permission_required('new_route_name')
   def new_route_name():
       # Your route code
   ```

3. Update navigation in `templates/base.html` if needed

The permission will automatically appear in the permission management interface.

