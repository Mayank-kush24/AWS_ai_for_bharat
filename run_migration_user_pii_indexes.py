"""
Migration script to add indexes to user_pii table for faster search and queries
This improves performance for the users list page and search functionality
"""
from database import db_manager

def run_migration():
    """Run the migration to add indexes to user_pii table"""
    try:
        # Read migration SQL
        with open('migration_add_user_pii_indexes.sql', 'r') as f:
            migration_sql = f.read()
        
        # Execute migration
        print("Running migration: Adding indexes to user_pii table...")
        print("This will improve search and query performance.")
        
        # Use connection directly to execute multiple statements
        conn = db_manager.get_connection()
        try:
            cursor = conn.cursor()
            # Execute the entire migration SQL (psycopg2 can handle multiple statements)
            cursor.execute(migration_sql)
            conn.commit()
            cursor.close()
        
            print("\nMigration completed successfully!")
            print("Indexes added:")
            print("  - idx_user_pii_name (on name column)")
            print("  - idx_user_pii_phone_number (on phone_number column)")
            print("  - idx_user_pii_country (on country column)")
            print("  - idx_user_pii_created_at (on created_at column)")
            print("  - idx_user_pii_search (composite index on name, email, country)")
        except Exception as e:
            conn.rollback()
            print(f"Error running migration: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            db_manager.return_connection(conn)

if __name__ == '__main__':
    run_migration()

