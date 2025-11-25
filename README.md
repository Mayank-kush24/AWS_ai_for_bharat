# AWS AI for Bharat Tracking System

A comprehensive tracking system for the AWS AI for Bharat initiative that connects to a local PostgreSQL database and automatically logs all activities to a master logs table.

## Features

- **Complete Database Schema**: All tables with proper primary keys and foreign keys
- **Automatic Activity Logging**: Every INSERT, UPDATE, and DELETE operation is automatically logged to the Master Logs table
- **PostgreSQL Integration**: Direct connection to local PostgreSQL database
- **Web Application**: Modern Flask web interface with beautiful UI
- **Data Import**: Import master XLSX files (12 sheets) and User PII data with validation
- **CLI Application**: Easy-to-use command-line interface for database operations

## Database Schema

The system includes the following tables:

1. **user_pii** - User Personal Identifiable Information (Primary Key: email)
2. **form_response** - Form responses (Composite Key: email, pk)
3. **aws_team_building** - AWS team building records (Composite Key: workshop_name, email)
4. **project_submission** - Project submissions (Composite Key: workshop_name, email)
5. **verification** - Verification records (Composite Key: workshop_name, email)
6. **master_logs** - Automatic activity tracking (Primary Key: log_id)

### Automatic Logging

The `master_logs` table automatically tracks:
- **Operation Type**: INSERT, UPDATE, or DELETE
- **Table Name**: Which table was modified
- **Record Identifier**: The primary/composite key of the affected record
- **Old Values**: Previous data (for UPDATE/DELETE operations)
- **New Values**: New data (for INSERT/UPDATE operations)
- **Timestamp**: When the operation occurred

All logging is handled by PostgreSQL triggers - no application code needed!

## Setup Instructions

### Prerequisites

- Python 3.7 or higher
- PostgreSQL server installed and running locally
- PostgreSQL database created

### Installation

1. **Clone or download this repository**

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Create a `.env` file** (copy from `env.example`):
   ```bash
   # On Windows PowerShell:
   Copy-Item env.example .env
   
   # On Linux/Mac:
   cp env.example .env
   ```

4. **Configure your database connection** in `.env`:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=aws_ai_bharat
   DB_USER=postgres
   DB_PASSWORD=your_password_here
   ```

5. **Create the PostgreSQL database** (if not exists):
   ```sql
   CREATE DATABASE aws_ai_bharat;
   ```

6. **Initialize the database schema**:
   - Option 1: Run the SQL file directly:
     ```bash
     psql -U postgres -d aws_ai_bharat -f schema.sql
     ```
   - Option 2: Use the CLI application (option 7 in menu)
   - Option 3: The web app will attempt to connect automatically

## Usage

### Running the Web Application

**Start the Flask web server:**

```bash
python app_web.py
```

The web application will be available at `http://localhost:4000`

**Features:**
- **Dashboard**: Overview with statistics and recent activity
- **User Management**: Create, view, edit, and list all users
- **Form Responses**: Manage form response records
- **Team Building**: Track AWS team building activities
- **Project Submissions**: Manage project submissions
- **Verifications**: Track verification records
- **Activity Logs**: View all automatic activity tracking from Master Logs
- **Data Import**: Import master XLSX files (12 sheets) and User PII data with full validation and error reporting

### Running the CLI Application

Alternatively, you can use the command-line interface:

```bash
python app.py
```

The CLI application provides a menu-driven interface for:
- User PII operations (create, read, update, list)
- Form Response operations
- AWS Team Building operations
- Project Submission operations
- Verification operations
- Viewing Master Logs (activity tracking)

### Importing Data

The web application includes a comprehensive import system:

1. **Master Workshops File**: Upload an XLSX file with 12 sheets containing workshop data
   - Sheets must follow the sequence: Project 1, Form 1, Form 2, Project 2, etc.
   - The system automatically detects sheet types and maps to correct workshops
   - Validates headers, emails, dates, and data integrity

2. **User PII File**: Upload an XLSX file containing user personal information
   - Automatically creates or updates user records
   - Validates email formats and data types

**Import Features:**
- Drag & drop file upload
- Real-time progress tracking
- Comprehensive validation (headers, emails, dates, duplicates)
- Detailed error reporting with row numbers
- Sheet-by-sheet summary with insert/update counts
- Automatic mapping to database tables

Access the import page at: `http://localhost:4000/import`

### Direct Database Access

You can also use the database models directly in your Python code:

```python
from database import UserPII, MasterLogs

# Create a user
UserPII.create(
    email="user@example.com",
    name="John Doe",
    phone_number="1234567890",
    country="India"
)

# View activity logs
logs = MasterLogs.get_all(limit=50)
for log in logs:
    print(f"{log['timestamp']}: {log['operation_type']} on {log['table_name']}")
```

### Querying Master Logs

The master logs table can be queried directly in PostgreSQL:

```sql
-- View all recent activities
SELECT * FROM master_logs ORDER BY timestamp DESC LIMIT 50;

-- View activities for a specific table
SELECT * FROM master_logs WHERE table_name = 'user_pii' ORDER BY timestamp DESC;

-- View all updates
SELECT * FROM master_logs WHERE operation_type = 'UPDATE' ORDER BY timestamp DESC;

-- View activity for a specific record
SELECT * FROM master_logs 
WHERE table_name = 'user_pii' AND record_identifier = 'user@example.com'
ORDER BY timestamp DESC;
```

## Database Schema Details

### Foreign Key Relationships

- All tables (form_response, aws_team_building, project_submission, verification) reference `user_pii.email` as a foreign key
- Foreign keys are set with `ON DELETE CASCADE` and `ON UPDATE CASCADE` to maintain referential integrity

### Automatic Timestamps

- All tables have `created_at` and `updated_at` columns
- `updated_at` is automatically updated via triggers when records are modified

### Master Logs Structure

The master_logs table stores:
- `log_id`: Auto-incrementing primary key
- `table_name`: Name of the table that was modified
- `operation_type`: INSERT, UPDATE, or DELETE
- `record_identifier`: Composite key or primary key value
- `old_values`: JSONB containing previous values (for UPDATE/DELETE)
- `new_values`: JSONB containing new values (for INSERT/UPDATE)
- `timestamp`: When the operation occurred
- `changed_by`: Optional field for tracking who made the change
- `additional_info`: Optional JSONB for extra metadata

## Troubleshooting

### Connection Issues

- Ensure PostgreSQL is running: `pg_isready` or check service status
- Verify database credentials in `.env` file
- Check if the database exists: `psql -U postgres -l`

### Schema Initialization Issues

- Ensure you have proper permissions on the database
- Check PostgreSQL logs for detailed error messages
- Verify that the `schema.sql` file is in the same directory

## License

This project is for the AWS AI for Bharat initiative.

