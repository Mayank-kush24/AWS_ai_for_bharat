"""
Migration script to add likes and comments columns to project_submission table
"""
from database import db_manager

def run_migration():
    """Run the migration to add likes and comments columns"""
    try:
        # Read migration SQL
        with open('migration_add_likes_comments.sql', 'r') as f:
            migration_sql = f.read()
        
        # Execute migration
        print("Running migration: Adding likes and comments columns...")
        db_manager.execute_query(migration_sql, fetch=False)
        print("Migration completed successfully!")
        
    except Exception as e:
        print(f"Error running migration: {e}")
        raise

if __name__ == '__main__':
    run_migration()

