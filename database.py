"""
Database connection and configuration for AWS AI for Bharat Tracking System
"""
import os
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
import json
from datetime import datetime

# Load environment variables
load_dotenv()


class DatabaseConfig:
    """Database configuration from environment variables"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'aws_ai_bharat')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.config = DatabaseConfig()
        self.pool: Optional[SimpleConnectionPool] = None
    
    def create_pool(self, min_conn: int = 1, max_conn: int = 10):
        """Create a connection pool"""
        try:
            self.pool = SimpleConnectionPool(
                min_conn,
                max_conn,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
            if self.pool:
                print("Connection pool created successfully")
        except Exception as e:
            print(f"Error creating connection pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        if not self.pool:
            self.create_pool()
        return self.pool.getconn()
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if self.pool:
            self.pool.putconn(conn)
    
    def close_pool(self):
        """Close all connections in the pool"""
        if self.pool:
            self.pool.closeall()
            print("Connection pool closed")
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = True):
        """Execute a query and return results"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, params)
            
            if fetch:
                if query.strip().upper().startswith('SELECT'):
                    result = cursor.fetchall()
                    return [dict(row) for row in result]
                else:
                    conn.commit()
                    return cursor.rowcount
            else:
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error executing query: {e}")
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    def initialize_database(self, schema_file: str = 'schema.sql'):
        """Initialize database by running schema SQL file"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            cursor.execute(schema_sql)
            conn.commit()
            print("Database schema initialized successfully")
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error initializing database: {e}")
            raise
        finally:
            if conn:
                self.return_connection(conn)


# Global database manager instance
db_manager = DatabaseManager()


class UserPII:
    """Model for User PII table"""
    
    @staticmethod
    def create(email: str, name: str, **kwargs):
        """Create a new user PII record"""
        query = """
            INSERT INTO user_pii (
                email, name, registration_date_time, phone_number, gender,
                country, state, city, date_of_birth, designation, class_stream,
                degree_passout_year, occupation, linkedin, participated_in_academy_1_0
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        params = (
            email, name,
            kwargs.get('registration_date_time', datetime.now()),
            kwargs.get('phone_number'),
            kwargs.get('gender'),
            kwargs.get('country'),
            kwargs.get('state'),
            kwargs.get('city'),
            kwargs.get('date_of_birth'),
            kwargs.get('designation'),
            kwargs.get('class_stream'),
            kwargs.get('degree_passout_year'),
            kwargs.get('occupation'),
            kwargs.get('linkedin'),
            kwargs.get('participated_in_academy_1_0', False)
        )
        return db_manager.execute_query(query, params, fetch=False)
    
    @staticmethod
    def get(email: str):
        """Get user PII by email"""
        query = "SELECT * FROM user_pii WHERE email = %s"
        result = db_manager.execute_query(query, (email,))
        return result[0] if result else None
    
    @staticmethod
    def update(email: str, **kwargs):
        """Update user PII record"""
        set_clauses = []
        params = []
        
        for key, value in kwargs.items():
            if value is not None:
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        if not set_clauses:
            return 0
        
        params.append(email)
        query = f"UPDATE user_pii SET {', '.join(set_clauses)} WHERE email = %s"
        return db_manager.execute_query(query, tuple(params), fetch=False)
    
    @staticmethod
    def list_all():
        """Get all user PII records"""
        query = "SELECT * FROM user_pii ORDER BY created_at DESC"
        return db_manager.execute_query(query)
    
    @staticmethod
    def bulk_upsert(records: list):
        """Bulk upsert user PII records (insert or update)"""
        if not records:
            return {"inserted": 0, "updated": 0}
        
        conn = None
        inserted = 0
        updated = 0
        try:
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            for record in records:
                # Check if user exists
                check_query = "SELECT email FROM user_pii WHERE email = %s"
                cursor.execute(check_query, (record['email'],))
                exists = cursor.fetchone()
                
                if exists:
                    # Update
                    update_query = """
                        UPDATE user_pii SET
                            name = %s, phone_number = %s, gender = %s,
                            country = %s, state = %s, city = %s,
                            date_of_birth = %s, designation = %s,
                            class_stream = %s, degree_passout_year = %s,
                            occupation = %s, linkedin = %s,
                            participated_in_academy_1_0 = %s,
                            registration_date_time = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE email = %s
                    """
                    cursor.execute(update_query, (
                        record.get('name'),
                        record.get('phone_number'),
                        record.get('gender'),
                        record.get('country'),
                        record.get('state'),
                        record.get('city'),
                        record.get('date_of_birth'),
                        record.get('designation'),
                        record.get('class_stream'),
                        record.get('degree_passout_year'),
                        record.get('occupation'),
                        record.get('linkedin'),
                        record.get('participated_in_academy_1_0', False),
                        record.get('registration_date_time'),
                        record['email']
                    ))
                    updated += cursor.rowcount
                else:
                    # Insert
                    insert_query = """
                        INSERT INTO user_pii (
                            email, name, registration_date_time, phone_number,
                            gender, country, state, city, date_of_birth,
                            designation, class_stream, degree_passout_year,
                            occupation, linkedin, participated_in_academy_1_0
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record['email'],
                        record.get('name'),
                        record.get('registration_date_time'),
                        record.get('phone_number'),
                        record.get('gender'),
                        record.get('country'),
                        record.get('state'),
                        record.get('city'),
                        record.get('date_of_birth'),
                        record.get('designation'),
                        record.get('class_stream'),
                        record.get('degree_passout_year'),
                        record.get('occupation'),
                        record.get('linkedin'),
                        record.get('participated_in_academy_1_0', False)
                    ))
                    inserted += cursor.rowcount
            
            conn.commit()
            return {"inserted": inserted, "updated": updated}
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                db_manager.return_connection(conn)


class FormResponse:
    """Model for Form Response table"""
    
    @staticmethod
    def create(email: str, form_name: str, name: str, **kwargs):
        """Create a new form response"""
        query = """
            INSERT INTO form_response (email, form_name, name, time_slot)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email, form_name) DO UPDATE SET
                name = EXCLUDED.name,
                time_slot = EXCLUDED.time_slot,
                updated_at = CURRENT_TIMESTAMP
            RETURNING email, form_name
        """
        params = (email, form_name, name, kwargs.get('time_slot'))
        result = db_manager.execute_query(query, params)
        return result[0] if result else None
    
    @staticmethod
    def get(email: str, form_name: str):
        """Get form response by email and form_name"""
        query = "SELECT * FROM form_response WHERE email = %s AND form_name = %s"
        result = db_manager.execute_query(query, (email, form_name))
        return result[0] if result else None
    
    @staticmethod
    def get_by_email(email: str):
        """Get all form responses for an email"""
        query = "SELECT * FROM form_response WHERE email = %s ORDER BY created_at DESC"
        return db_manager.execute_query(query, (email,))
    
    @staticmethod
    def bulk_upsert(records: list):
        """Bulk upsert form response records"""
        if not records:
            return {"inserted": 0, "updated": 0}
        
        conn = None
        inserted = 0
        updated = 0
        try:
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            for record in records:
                # Insert new form response (form_response allows multiple entries per email)
                insert_query = """
                    INSERT INTO form_response (email, form_name, name, time_slot)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    record['email'],
                    record.get('workshop_name', ''),
                    record.get('name'),
                    record.get('time_slot')
                ))
                inserted += cursor.rowcount
            
            conn.commit()
            return {"inserted": inserted, "updated": updated}
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                db_manager.return_connection(conn)


class AWSTeamBuilding:
    """Model for AWS Team Building table"""
    
    @staticmethod
    def create(workshop_name: str, email: str, name: str, **kwargs):
        """Create a new AWS team building record"""
        query = """
            INSERT INTO aws_team_building (
                workshop_name, email, name, workshop_link, team_id
            ) VALUES (%s, %s, %s, %s, %s)
        """
        params = (
            workshop_name, email, name,
            kwargs.get('workshop_link'),
            kwargs.get('team_id')
        )
        return db_manager.execute_query(query, params, fetch=False)
    
    @staticmethod
    def get(workshop_name: str, email: str):
        """Get AWS team building record"""
        query = "SELECT * FROM aws_team_building WHERE workshop_name = %s AND email = %s"
        result = db_manager.execute_query(query, (workshop_name, email))
        return result[0] if result else None
    
    @staticmethod
    def update(workshop_name: str, email: str, **kwargs):
        """Update AWS team building record"""
        set_clauses = []
        params = []
        
        for key, value in kwargs.items():
            if value is not None:
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        if not set_clauses:
            return 0
        
        params.extend([workshop_name, email])
        query = f"UPDATE aws_team_building SET {', '.join(set_clauses)} WHERE workshop_name = %s AND email = %s"
        return db_manager.execute_query(query, tuple(params), fetch=False)
    
    @staticmethod
    def list_all():
        """Get all AWS team building records"""
        query = "SELECT * FROM aws_team_building ORDER BY created_at DESC"
        return db_manager.execute_query(query)
    
    @staticmethod
    def bulk_upsert(records: list):
        """Bulk upsert AWS team building records"""
        if not records:
            return {"inserted": 0, "updated": 0}
        
        conn = None
        inserted = 0
        updated = 0
        try:
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            for record in records:
                # Check if exists
                check_query = "SELECT workshop_name, email FROM aws_team_building WHERE workshop_name = %s AND email = %s"
                cursor.execute(check_query, (record['workshop_name'], record['email']))
                exists = cursor.fetchone()
                
                if exists:
                    # Update
                    update_query = """
                        UPDATE aws_team_building SET
                            name = %s, workshop_link = %s, team_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE workshop_name = %s AND email = %s
                    """
                    cursor.execute(update_query, (
                        record.get('name'),
                        record.get('workshop_link'),
                        record.get('team_id'),
                        record['workshop_name'],
                        record['email']
                    ))
                    updated += cursor.rowcount
                else:
                    # Insert
                    insert_query = """
                        INSERT INTO aws_team_building (workshop_name, email, name, workshop_link, team_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record['workshop_name'],
                        record['email'],
                        record.get('name'),
                        record.get('workshop_link'),
                        record.get('team_id')
                    ))
                    inserted += cursor.rowcount
            
            conn.commit()
            return {"inserted": inserted, "updated": updated}
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                db_manager.return_connection(conn)


class ProjectSubmission:
    """Model for Project Submission table"""
    
    @staticmethod
    def create(workshop_name: str, email: str, name: str, **kwargs):
        """Create a new project submission"""
        query = """
            INSERT INTO project_submission (
                workshop_name, email, name, project_link, valid, team_id, validation_reason, likes, comments
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            workshop_name, email, name,
            kwargs.get('project_link'),
            kwargs.get('valid', False),
            kwargs.get('team_id'),
            kwargs.get('validation_reason'),
            kwargs.get('likes', 0),
            kwargs.get('comments', 0)
        )
        return db_manager.execute_query(query, params, fetch=False)
    
    @staticmethod
    def get(workshop_name: str, email: str):
        """Get project submission"""
        query = "SELECT * FROM project_submission WHERE workshop_name = %s AND email = %s"
        result = db_manager.execute_query(query, (workshop_name, email))
        return result[0] if result else None
    
    @staticmethod
    def update(workshop_name: str, email: str, **kwargs):
        """Update project submission"""
        set_clauses = []
        params = []
        
        for key, value in kwargs.items():
            if value is not None:
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        if not set_clauses:
            return 0
        
        params.extend([workshop_name, email])
        query = f"UPDATE project_submission SET {', '.join(set_clauses)} WHERE workshop_name = %s AND email = %s"
        return db_manager.execute_query(query, tuple(params), fetch=False)
    
    @staticmethod
    def list_all():
        """Get all project submissions"""
        query = "SELECT * FROM project_submission ORDER BY created_at DESC"
        return db_manager.execute_query(query)
    
    @staticmethod
    def bulk_upsert(records: list):
        """Bulk upsert project submission records"""
        if not records:
            return {"inserted": 0, "updated": 0}
        
        conn = None
        inserted = 0
        updated = 0
        try:
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            for record in records:
                # Check if exists
                check_query = "SELECT workshop_name, email FROM project_submission WHERE workshop_name = %s AND email = %s"
                cursor.execute(check_query, (record['workshop_name'], record['email']))
                exists = cursor.fetchone()
                
                if exists:
                    # Update
                    update_query = """
                        UPDATE project_submission SET
                            name = %s, project_link = %s, valid = %s,
                            team_id = %s, validation_reason = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE workshop_name = %s AND email = %s
                    """
                    cursor.execute(update_query, (
                        record.get('name'),
                        record.get('project_link'),
                        record.get('valid', False),
                        record.get('team_id'),
                        record.get('validation_reason'),
                        record['workshop_name'],
                        record['email']
                    ))
                    updated += cursor.rowcount
                else:
                    # Insert
                    insert_query = """
                        INSERT INTO project_submission (workshop_name, email, name, project_link, valid, team_id, validation_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record['workshop_name'],
                        record['email'],
                        record.get('name'),
                        record.get('project_link'),
                        record.get('valid', False),
                        record.get('team_id'),
                        record.get('validation_reason')
                    ))
                    inserted += cursor.rowcount
            
            conn.commit()
            return {"inserted": inserted, "updated": updated}
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                db_manager.return_connection(conn)


class Verification:
    """Model for Verification table"""
    
    @staticmethod
    def create(workshop_name: str, email: str, name: str, **kwargs):
        """Create a new verification record"""
        query = """
            INSERT INTO verification (
                workshop_name, email, name, project_ss, project_valid,
                blog, blog_valid, team_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            workshop_name, email, name,
            kwargs.get('project_ss'),
            kwargs.get('project_valid', False),
            kwargs.get('blog'),
            kwargs.get('blog_valid', False),
            kwargs.get('team_id')
        )
        return db_manager.execute_query(query, params, fetch=False)
    
    @staticmethod
    def get(workshop_name: str, email: str):
        """Get verification record"""
        query = "SELECT * FROM verification WHERE workshop_name = %s AND email = %s"
        result = db_manager.execute_query(query, (workshop_name, email))
        return result[0] if result else None
    
    @staticmethod
    def update(workshop_name: str, email: str, **kwargs):
        """Update verification record"""
        set_clauses = []
        params = []
        
        for key, value in kwargs.items():
            if value is not None:
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        if not set_clauses:
            return 0
        
        params.extend([workshop_name, email])
        query = f"UPDATE verification SET {', '.join(set_clauses)} WHERE workshop_name = %s AND email = %s"
        return db_manager.execute_query(query, tuple(params), fetch=False)
    
    @staticmethod
    def list_all():
        """Get all verification records"""
        query = "SELECT * FROM verification ORDER BY created_at DESC"
        return db_manager.execute_query(query)


class KiroSubmission:
    """Model for Kiro Submission table"""
    
    @staticmethod
    def create(week_number: int, email: str, **kwargs):
        """Create a new kiro submission"""
        query = """
            INSERT INTO kiro_submission (week_number, email, github_link, blog_link)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (week_number, email) DO UPDATE SET
                github_link = EXCLUDED.github_link,
                blog_link = EXCLUDED.blog_link,
                updated_at = CURRENT_TIMESTAMP
            RETURNING week_number, email
        """
        params = (
            week_number,
            email,
            kwargs.get('github_link'),
            kwargs.get('blog_link')
        )
        result = db_manager.execute_query(query, params)
        return result[0] if result else None
    
    @staticmethod
    def get(week_number: int, email: str):
        """Get kiro submission by week_number and email"""
        query = "SELECT * FROM kiro_submission WHERE week_number = %s AND email = %s"
        result = db_manager.execute_query(query, (week_number, email))
        return result[0] if result else None
    
    @staticmethod
    def update(week_number: int, email: str, **kwargs):
        """Update kiro submission"""
        set_clauses = []
        params = []
        
        for key, value in kwargs.items():
            if value is not None:
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        if not set_clauses:
            return 0
        
        params.extend([week_number, email])
        query = f"UPDATE kiro_submission SET {', '.join(set_clauses)} WHERE week_number = %s AND email = %s"
        return db_manager.execute_query(query, tuple(params), fetch=False)
    
    @staticmethod
    def delete(week_number: int, email: str):
        """Delete kiro submission"""
        query = "DELETE FROM kiro_submission WHERE week_number = %s AND email = %s"
        return db_manager.execute_query(query, (week_number, email), fetch=False)
    
    @staticmethod
    def get_by_week(week_number: int):
        """Get all submissions for a specific week"""
        query = """
            SELECT ks.*, u.name, u.phone_number, u.country
            FROM kiro_submission ks
            LEFT JOIN user_pii u ON ks.email = u.email
            WHERE ks.week_number = %s
            ORDER BY ks.created_at DESC
        """
        return db_manager.execute_query(query, (week_number,))
    
    @staticmethod
    def get_by_email(email: str):
        """Get all submissions for a specific email"""
        query = """
            SELECT * FROM kiro_submission
            WHERE email = %s
            ORDER BY week_number ASC
        """
        return db_manager.execute_query(query, (email,))
    
    @staticmethod
    def list_all():
        """Get all kiro submissions"""
        query = """
            SELECT ks.*, u.name
            FROM kiro_submission ks
            LEFT JOIN user_pii u ON ks.email = u.email
            ORDER BY ks.week_number DESC, ks.created_at DESC
        """
        return db_manager.execute_query(query)
    
    @staticmethod
    def get_weeks():
        """Get list of all unique week numbers"""
        query = "SELECT DISTINCT week_number FROM kiro_submission ORDER BY week_number ASC"
        result = db_manager.execute_query(query)
        return [row['week_number'] for row in result] if result else []
    
    @staticmethod
    def get_top_participants(week_number: int, limit: int = 10):
        """Get top participants for a week based on valid GitHub and highest engagement (likes + comments)
        
        Args:
            week_number: Week number to filter by
            limit: Number of top participants to return
            
        Returns:
            List of submission records sorted by engagement (likes + comments) descending
        """
        # Ensure limit is a valid integer
        limit = int(limit) if limit else 10
        if limit < 1:
            limit = 10
        
        query = """
            SELECT ks.*, 
                   u.name, u.phone_number, u.country, u.state, u.city, 
                   u.linkedin, u.gender, u.designation, u.occupation, 
                   u.class_stream, u.degree_passout_year, u.date_of_birth,
                   u.participated_in_academy_1_0, u.registration_date_time
            FROM kiro_submission ks
            LEFT JOIN user_pii u ON ks.email = u.email
            WHERE ks.week_number = %s 
                AND ks.github_valid = TRUE
                AND (COALESCE(ks.likes, 0) + COALESCE(ks.comments, 0)) > 0
            ORDER BY (COALESCE(ks.likes, 0) + COALESCE(ks.comments, 0)) DESC, ks.likes DESC, ks.comments DESC
            LIMIT %s
        """
        print(f"[DEBUG] get_top_participants called with week_number={week_number}, limit={limit}")
        result = db_manager.execute_query(query, (week_number, limit))
        print(f"[DEBUG] get_top_participants returned {len(result) if result else 0} records")
        return result
    
    @staticmethod
    def bulk_upsert(records: list, mode: str = 'upsert'):
        """Bulk upsert kiro submission records
        
        Args:
            records: List of records to import
            mode: 'create' (insert only), 'update' (update only), or 'upsert' (insert or update)
        
        Returns:
            dict with 'inserted' and 'updated' counts
        """
        if not records:
            return {'inserted': 0, 'updated': 0}
        
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        try:
            inserted = 0
            updated = 0
            
            if mode == 'create':
                # Create only - skip if exists
                query = """
                    INSERT INTO kiro_submission (week_number, email, github_link, blog_link, created_at, updated_at, valid, validation_reason, likes, comments)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (week_number, email) DO NOTHING
                """
                for record in records:
                    try:
                        cursor.execute(query, (
                            record.get('week_number'),
                            record.get('email'),
                            record.get('github_link'),
                            record.get('blog_link'),
                            record.get('created_at'),  # Will use default if None
                            record.get('updated_at'),   # Will use default if None
                            record.get('valid', False),
                            record.get('validation_reason'),
                            record.get('likes', 0),
                            record.get('comments', 0)
                        ))
                        if cursor.rowcount > 0:
                            inserted += 1
                    except Exception as e:
                        error_msg = f"Error inserting record (week={record.get('week_number')}, email={record.get('email')}): {str(e)}"
                        print(error_msg)
                        raise Exception(error_msg)  # Re-raise to be caught by caller
                        
            elif mode == 'update':
                # Update only - skip if doesn't exist
                # Build dynamic UPDATE query based on what fields are provided
                for record in records:
                    try:
                        set_clauses = []
                        params = []
                        
                        if record.get('github_link') is not None:
                            set_clauses.append("github_link = %s")
                            params.append(record.get('github_link'))
                        
                        if record.get('blog_link') is not None:
                            set_clauses.append("blog_link = %s")
                            params.append(record.get('blog_link'))
                        
                        if record.get('valid') is not None:
                            set_clauses.append("valid = %s")
                            params.append(record.get('valid'))
                        
                        if record.get('validation_reason') is not None:
                            set_clauses.append("validation_reason = %s")
                            params.append(record.get('validation_reason'))
                        
                        if record.get('likes') is not None:
                            set_clauses.append("likes = %s")
                            params.append(record.get('likes'))
                        
                        if record.get('comments') is not None:
                            set_clauses.append("comments = %s")
                            params.append(record.get('comments'))
                        
                        if record.get('updated_at') is not None:
                            set_clauses.append("updated_at = %s")
                            params.append(record.get('updated_at'))
                        else:
                            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
                        
                        if not set_clauses:
                            continue
                        
                        params.extend([record.get('week_number'), record.get('email')])
                        query = f"""
                            UPDATE kiro_submission
                            SET {', '.join(set_clauses)}
                            WHERE week_number = %s AND email = %s
                        """
                        cursor.execute(query, tuple(params))
                        if cursor.rowcount > 0:
                            updated += 1
                    except Exception as e:
                        print(f"Error updating record {record}: {e}")
                        continue
                        
            else:  # mode == 'upsert'
                # Create or update - check if exists first to count properly
                check_query = "SELECT 1 FROM kiro_submission WHERE week_number = %s AND email = %s"
                query = """
                    INSERT INTO kiro_submission (week_number, email, github_link, blog_link, created_at, updated_at, valid, validation_reason, likes, comments)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (week_number, email) DO UPDATE SET
                        github_link = EXCLUDED.github_link,
                        blog_link = EXCLUDED.blog_link,
                        updated_at = COALESCE(EXCLUDED.updated_at, CURRENT_TIMESTAMP),
                        valid = COALESCE(EXCLUDED.valid, kiro_submission.valid),
                        validation_reason = COALESCE(EXCLUDED.validation_reason, kiro_submission.validation_reason),
                        likes = COALESCE(EXCLUDED.likes, kiro_submission.likes),
                        comments = COALESCE(EXCLUDED.comments, kiro_submission.comments)
                """
                for record in records:
                    try:
                        # Check if record exists before insert
                        cursor.execute(check_query, (
                            record.get('week_number'),
                            record.get('email')
                        ))
                        exists = cursor.fetchone() is not None
                        
                        # Perform insert/update
                        cursor.execute(query, (
                            record.get('week_number'),
                            record.get('email'),
                            record.get('github_link'),
                            record.get('blog_link'),
                            record.get('created_at'),  # Will use default if None
                            record.get('updated_at'),   # Will use default if None
                            record.get('valid', False),
                            record.get('validation_reason'),
                            record.get('likes', 0),
                            record.get('comments', 0)
                        ))
                        
                        # Count based on whether it existed before
                        if exists:
                            updated += 1
                        else:
                            inserted += 1
                    except Exception as e:
                        error_msg = f"Error upserting record (week={record.get('week_number')}, email={record.get('email')}): {str(e)}"
                        print(error_msg)
                        raise Exception(error_msg)  # Re-raise to be caught by caller
            
            conn.commit()
            return {'inserted': inserted, 'updated': updated}
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            db_manager.return_connection(conn)


class MasterLogs:
    """Model for Master Logs table (read-only queries)"""
    
    @staticmethod
    def get_all(limit: int = 100, offset: int = 0):
        """Get all master logs with pagination"""
        query = """
            SELECT * FROM master_logs 
            ORDER BY timestamp DESC 
            LIMIT %s OFFSET %s
        """
        return db_manager.execute_query(query, (limit, offset))
    
    @staticmethod
    def get_by_table(table_name: str, limit: int = 100):
        """Get logs for a specific table"""
        query = """
            SELECT * FROM master_logs 
            WHERE table_name = %s 
            ORDER BY timestamp DESC 
            LIMIT %s
        """
        return db_manager.execute_query(query, (table_name, limit))
    
    @staticmethod
    def get_by_operation(operation_type: str, limit: int = 100):
        """Get logs by operation type (INSERT, UPDATE, DELETE)"""
        query = """
            SELECT * FROM master_logs 
            WHERE operation_type = %s 
            ORDER BY timestamp DESC 
            LIMIT %s
        """
        return db_manager.execute_query(query, (operation_type, limit))
    
    @staticmethod
    def get_by_date_range(start_date: datetime, end_date: datetime, limit: int = 100):
        """Get logs within a date range"""
        query = """
            SELECT * FROM master_logs 
            WHERE timestamp BETWEEN %s AND %s 
            ORDER BY timestamp DESC 
            LIMIT %s
        """
        return db_manager.execute_query(query, (start_date, end_date, limit))
    
    @staticmethod
    def get_by_record(table_name: str, record_identifier: str):
        """Get all logs for a specific record"""
        query = """
            SELECT * FROM master_logs 
            WHERE table_name = %s AND record_identifier = %s 
            ORDER BY timestamp DESC
        """
        return db_manager.execute_query(query, (table_name, record_identifier))


# ============================================
# RBAC (Role-Based Access Control) Models
# ============================================

class RBACUser:
    """RBAC User model for authentication and authorization"""
    
    @staticmethod
    def create(username: str, email: str, password: str, full_name: str = None, is_admin: bool = False):
        """Create a new RBAC user"""
        import bcrypt
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        conn = db_manager.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            query = """
                INSERT INTO rbac_users (username, email, password_hash, full_name, is_admin)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING user_id, username, email, full_name, is_admin, is_active, created_at
            """
            cursor.execute(query, (username, email, password_hash, full_name, is_admin))
            result = cursor.fetchone()
            conn.commit()
            return dict(result) if result else None
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            db_manager.return_connection(conn)
    
    @staticmethod
    def get_by_username(username: str):
        """Get user by username"""
        query = "SELECT * FROM rbac_users WHERE username = %s"
        result = db_manager.execute_query(query, (username,))
        return result[0] if result else None
    
    @staticmethod
    def get_by_email(email: str):
        """Get user by email"""
        query = "SELECT * FROM rbac_users WHERE email = %s"
        result = db_manager.execute_query(query, (email,))
        return result[0] if result else None
    
    @staticmethod
    def get_by_id(user_id: int):
        """Get user by ID"""
        query = "SELECT * FROM rbac_users WHERE user_id = %s"
        result = db_manager.execute_query(query, (user_id,))
        return result[0] if result else None
    
    @staticmethod
    def verify_password(password_hash: str, password: str) -> bool:
        """Verify password against hash"""
        import bcrypt
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except:
            return False
    
    @staticmethod
    def update_last_login(user_id: int):
        """Update last login timestamp"""
        query = "UPDATE rbac_users SET last_login = CURRENT_TIMESTAMP WHERE user_id = %s"
        db_manager.execute_query(query, (user_id,))
    
    @staticmethod
    def list_all():
        """List all users"""
        query = "SELECT user_id, username, email, full_name, is_admin, is_active, created_at, last_login FROM rbac_users ORDER BY created_at DESC"
        return db_manager.execute_query(query)
    
    @staticmethod
    def update(user_id: int, username: str = None, email: str = None, full_name: str = None, 
               is_admin: bool = None, is_active: bool = None, password: str = None):
        """Update user"""
        updates = []
        params = []
        
        if username:
            updates.append("username = %s")
            params.append(username)
        if email:
            updates.append("email = %s")
            params.append(email)
        if full_name is not None:
            updates.append("full_name = %s")
            params.append(full_name)
        if is_admin is not None:
            updates.append("is_admin = %s")
            params.append(is_admin)
        if is_active is not None:
            updates.append("is_active = %s")
            params.append(is_active)
        if password:
            import bcrypt
            password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            updates.append("password_hash = %s")
            params.append(password_hash)
        
        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(user_id)
        
        conn = db_manager.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            query = f"UPDATE rbac_users SET {', '.join(updates)} WHERE user_id = %s RETURNING *"
            cursor.execute(query, tuple(params))
            result = cursor.fetchone()
            conn.commit()
            return dict(result) if result else None
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            db_manager.return_connection(conn)
    
    @staticmethod
    def delete(user_id: int):
        """Delete user"""
        query = "DELETE FROM rbac_users WHERE user_id = %s"
        db_manager.execute_query(query, (user_id,))


class RBACPermission:
    """RBAC Permission model"""
    
    @staticmethod
    def get_all():
        """Get all permissions"""
        query = "SELECT * FROM rbac_permissions ORDER BY category, display_name"
        return db_manager.execute_query(query)
    
    @staticmethod
    def get_by_route(route_name: str):
        """Get permission by route name"""
        query = "SELECT * FROM rbac_permissions WHERE route_name = %s"
        result = db_manager.execute_query(query, (route_name,))
        return result[0] if result else None
    
    @staticmethod
    def create(route_name: str, display_name: str, description: str = None, category: str = None):
        """Create a new permission"""
        conn = db_manager.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            query = """
                INSERT INTO rbac_permissions (route_name, display_name, description, category)
                VALUES (%s, %s, %s, %s)
                RETURNING *
            """
            cursor.execute(query, (route_name, display_name, description, category))
            result = cursor.fetchone()
            conn.commit()
            return dict(result) if result else None
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            db_manager.return_connection(conn)


class RBACUserPermission:
    """RBAC User Permission junction model"""
    
    @staticmethod
    def get_user_permissions(user_id: int):
        """Get all permissions for a user"""
        query = """
            SELECT p.* 
            FROM rbac_permissions p
            INNER JOIN rbac_user_permissions up ON p.permission_id = up.permission_id
            WHERE up.user_id = %s
            ORDER BY p.category, p.display_name
        """
        return db_manager.execute_query(query, (user_id,))
    
    @staticmethod
    def get_user_permission_routes(user_id: int):
        """Get list of route names for a user"""
        query = """
            SELECT p.route_name 
            FROM rbac_permissions p
            INNER JOIN rbac_user_permissions up ON p.permission_id = up.permission_id
            WHERE up.user_id = %s
        """
        results = db_manager.execute_query(query, (user_id,))
        if results:
            return [row.get('route_name') if isinstance(row, dict) else row[0] for row in results]
        return []
    
    @staticmethod
    def has_permission(user_id: int, route_name: str) -> bool:
        """Check if user has a specific permission"""
        # Admin users have all permissions
        user = RBACUser.get_by_id(user_id)
        if user and user.get('is_admin'):
            return True
        
        query = """
            SELECT COUNT(*) as count
            FROM rbac_user_permissions up
            INNER JOIN rbac_permissions p ON up.permission_id = p.permission_id
            WHERE up.user_id = %s AND p.route_name = %s
        """
        result = db_manager.execute_query(query, (user_id, route_name))
        if result and len(result) > 0:
            count = result[0].get('count', 0) if isinstance(result[0], dict) else result[0][0] if isinstance(result[0], (list, tuple)) else 0
            return count > 0
        return False
    
    @staticmethod
    def grant_permission(user_id: int, permission_id: int, granted_by: int = None):
        """Grant permission to user"""
        query = """
            INSERT INTO rbac_user_permissions (user_id, permission_id, granted_by)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, permission_id) DO NOTHING
        """
        db_manager.execute_query(query, (user_id, permission_id, granted_by))
    
    @staticmethod
    def revoke_permission(user_id: int, permission_id: int):
        """Revoke permission from user"""
        query = "DELETE FROM rbac_user_permissions WHERE user_id = %s AND permission_id = %s"
        db_manager.execute_query(query, (user_id, permission_id))
    
    @staticmethod
    def set_user_permissions(user_id: int, permission_ids: list, granted_by: int = None):
        """Set all permissions for a user (replaces existing)"""
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        try:
            # Remove all existing permissions
            cursor.execute("DELETE FROM rbac_user_permissions WHERE user_id = %s", (user_id,))
            
            # Add new permissions
            if permission_ids:
                for perm_id in permission_ids:
                    cursor.execute(
                        "INSERT INTO rbac_user_permissions (user_id, permission_id, granted_by) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (user_id, perm_id, granted_by)
                    )
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            db_manager.return_connection(conn)

