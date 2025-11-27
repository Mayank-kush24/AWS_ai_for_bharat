"""
Advanced database operations with flexible matching and import modes
"""
from datetime import datetime
from database import db_manager


def build_match_query(table_name: str, match_fields: list, record: dict) -> tuple:
    """Build WHERE clause for matching records"""
    match_conditions = []
    match_values = []
    
    for field in match_fields:
        if field in record and record[field] is not None:
            match_conditions.append(f"{field} = %s")
            match_values.append(record[field])
    
    return match_conditions, match_values


def bulk_upsert_advanced_user_pii(records: list, mode: str = 'upsert', match_fields: list = None):
    """Advanced bulk upsert for User PII with mode and match fields"""
    if not records:
        return {"inserted": 0, "updated": 0}
    
    if match_fields is None:
        match_fields = ['email']
    
    conn = None
    inserted = 0
    updated = 0
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        for record in records:
            match_conditions, match_values = build_match_query('user_pii', match_fields, record)
            
            if not match_conditions:
                continue
            
            # Check if exists
            check_query = f"SELECT email FROM user_pii WHERE {' AND '.join(match_conditions)}"
            cursor.execute(check_query, tuple(match_values))
            exists = cursor.fetchone()
            
            if mode == 'create':
                if not exists:
                    # Use default timestamp if registration_date_time is null
                    registration_dt = record.get('registration_date_time')
                    if registration_dt is None:
                        registration_dt = datetime.now()
                    
                    insert_query = """
                        INSERT INTO user_pii (
                            email, name, registration_date_time, phone_number,
                            gender, country, state, city, date_of_birth,
                            designation, class_stream, degree_passout_year,
                            occupation, linkedin, participated_in_academy_1_0
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record.get('email'),
                        record.get('name'),
                        registration_dt,
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
            
            elif mode == 'update':
                if exists:
                    where_clause = ' AND '.join(match_conditions)
                    update_query = f"""
                        UPDATE user_pii SET
                            name = %s, phone_number = %s, gender = %s,
                            country = %s, state = %s, city = %s,
                            date_of_birth = %s, designation = %s,
                            class_stream = %s, degree_passout_year = %s,
                            occupation = %s, linkedin = %s,
                            participated_in_academy_1_0 = %s,
                            registration_date_time = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    update_values = [
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
                        record.get('registration_date_time')
                    ] + match_values
                    cursor.execute(update_query, tuple(update_values))
                    updated += cursor.rowcount
            
            elif mode == 'upsert':
                if exists:
                    where_clause = ' AND '.join(match_conditions)
                    # Only update registration_date_time if provided, otherwise keep existing
                    registration_dt = record.get('registration_date_time')
                    if registration_dt is None:
                        # Don't update registration_date_time if null
                        update_query = f"""
                            UPDATE user_pii SET
                                name = %s, phone_number = %s, gender = %s,
                                country = %s, state = %s, city = %s,
                                date_of_birth = %s, designation = %s,
                                class_stream = %s, degree_passout_year = %s,
                                occupation = %s, linkedin = %s,
                                participated_in_academy_1_0 = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE {where_clause}
                        """
                        update_values = [
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
                            record.get('participated_in_academy_1_0', False)
                        ] + match_values
                    else:
                        update_query = f"""
                            UPDATE user_pii SET
                                name = %s, phone_number = %s, gender = %s,
                                country = %s, state = %s, city = %s,
                                date_of_birth = %s, designation = %s,
                                class_stream = %s, degree_passout_year = %s,
                                occupation = %s, linkedin = %s,
                                participated_in_academy_1_0 = %s,
                                registration_date_time = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE {where_clause}
                        """
                        update_values = [
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
                            registration_dt
                        ] + match_values
                    cursor.execute(update_query, tuple(update_values))
                    updated += cursor.rowcount
                else:
                    # Use default timestamp if registration_date_time is null
                    registration_dt = record.get('registration_date_time')
                    if registration_dt is None:
                        registration_dt = datetime.now()
                    
                    insert_query = """
                        INSERT INTO user_pii (
                            email, name, registration_date_time, phone_number,
                            gender, country, state, city, date_of_birth,
                            designation, class_stream, degree_passout_year,
                            occupation, linkedin, participated_in_academy_1_0
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record.get('email'),
                        record.get('name'),
                        registration_dt,
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


def bulk_upsert_advanced_form_response(records: list, mode: str = 'upsert', match_fields: list = None):
    """Advanced bulk upsert for Form Response"""
    if not records:
        return {"inserted": 0, "updated": 0}
    
    if match_fields is None:
        match_fields = ['email', 'form_name']  # Composite key
    
    conn = None
    inserted = 0
    updated = 0
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        for record in records:
            email = record.get('email')
            form_name = record.get('form_name')
            
            if not email or not form_name:
                continue  # Skip invalid records
            
            # Check if exists (using composite key)
            check_query = "SELECT email, form_name FROM form_response WHERE email = %s AND form_name = %s"
            cursor.execute(check_query, (email, form_name))
            exists = cursor.fetchone()
            
            if mode == 'create':
                if not exists:
                    insert_query = """
                        INSERT INTO form_response (email, form_name, name, time_slot, time_slot_range)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        email,
                        form_name,
                        record.get('name'),
                        record.get('time_slot'),
                        record.get('time_slot_original')
                    ))
                    inserted += cursor.rowcount
            
            elif mode == 'update':
                if exists:
                    update_query = """
                        UPDATE form_response SET
                            name = %s, time_slot = %s, time_slot_range = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE email = %s AND form_name = %s
                    """
                    cursor.execute(update_query, (
                        record.get('name'),
                        record.get('time_slot'),
                        record.get('time_slot_original'),
                        email,
                        form_name
                    ))
                    updated += cursor.rowcount
            
            elif mode == 'upsert':
                if exists:
                    update_query = """
                        UPDATE form_response SET
                            name = %s, time_slot = %s, time_slot_range = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE email = %s AND form_name = %s
                    """
                    cursor.execute(update_query, (
                        record.get('name'),
                        record.get('time_slot'),
                        record.get('time_slot_original'),
                        email,
                        form_name
                    ))
                    updated += cursor.rowcount
                else:
                    insert_query = """
                        INSERT INTO form_response (email, form_name, name, time_slot, time_slot_range)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        email,
                        form_name,
                        record.get('name'),
                        record.get('time_slot'),
                        record.get('time_slot_original')
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


def bulk_upsert_advanced_project_submission(records: list, mode: str = 'upsert', match_fields: list = None):
    """Advanced bulk upsert for Project Submission"""
    if not records:
        return {"inserted": 0, "updated": 0}
    
    if match_fields is None:
        match_fields = ['workshop_name', 'email']
    
    conn = None
    inserted = 0
    updated = 0
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        for record in records:
            match_conditions, match_values = build_match_query('project_submission', match_fields, record)
            
            if not match_conditions:
                continue
            
            check_query = f"SELECT workshop_name, email FROM project_submission WHERE {' AND '.join(match_conditions)}"
            cursor.execute(check_query, tuple(match_values))
            exists = cursor.fetchone()
            
            if mode == 'create':
                if not exists:
                    insert_query = """
                        INSERT INTO project_submission (workshop_name, email, name, project_link, valid, team_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record.get('workshop_name'),
                        record.get('email'),
                        record.get('name'),
                        record.get('project_link'),
                        record.get('valid', False),
                        record.get('team_id')
                    ))
                    inserted += cursor.rowcount
            
            elif mode == 'update':
                if exists:
                    where_clause = ' AND '.join(match_conditions)
                    update_query = f"""
                        UPDATE project_submission SET
                            name = %s, project_link = %s, valid = %s,
                            team_id = %s, likes = %s, comments = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    update_values = [
                        record.get('name'),
                        record.get('project_link'),
                        record.get('valid', False),
                        record.get('team_id'),
                        record.get('likes', 0),
                        record.get('comments', 0)
                    ] + match_values
                    cursor.execute(update_query, tuple(update_values))
                    updated += cursor.rowcount
            
            elif mode == 'upsert':
                if exists:
                    where_clause = ' AND '.join(match_conditions)
                    update_query = f"""
                        UPDATE project_submission SET
                            name = %s, project_link = %s, valid = %s,
                            team_id = %s, likes = %s, comments = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    update_values = [
                        record.get('name'),
                        record.get('project_link'),
                        record.get('valid', False),
                        record.get('team_id'),
                        record.get('likes', 0),
                        record.get('comments', 0)
                    ] + match_values
                    cursor.execute(update_query, tuple(update_values))
                    updated += cursor.rowcount
                else:
                    insert_query = """
                        INSERT INTO project_submission (workshop_name, email, name, project_link, valid, team_id, likes, comments)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record.get('workshop_name'),
                        record.get('email'),
                        record.get('name'),
                        record.get('project_link'),
                        record.get('valid', False),
                        record.get('team_id'),
                        record.get('likes', 0),
                        record.get('comments', 0)
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


def bulk_upsert_advanced_aws_team_building(records: list, mode: str = 'upsert', match_fields: list = None):
    """Advanced bulk upsert for AWS Team Building"""
    if not records:
        return {"inserted": 0, "updated": 0}
    
    if match_fields is None:
        match_fields = ['workshop_name', 'email']
    
    conn = None
    inserted = 0
    updated = 0
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        for record in records:
            match_conditions, match_values = build_match_query('aws_team_building', match_fields, record)
            
            if not match_conditions:
                continue
            
            check_query = f"SELECT workshop_name, email FROM aws_team_building WHERE {' AND '.join(match_conditions)}"
            cursor.execute(check_query, tuple(match_values))
            exists = cursor.fetchone()
            
            if mode == 'create':
                if not exists:
                    insert_query = """
                        INSERT INTO aws_team_building (workshop_name, email, name, workshop_link, team_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record.get('workshop_name'),
                        record.get('email'),
                        record.get('name'),
                        record.get('workshop_link'),
                        record.get('team_id')
                    ))
                    inserted += cursor.rowcount
            
            elif mode == 'update':
                if exists:
                    where_clause = ' AND '.join(match_conditions)
                    update_query = f"""
                        UPDATE aws_team_building SET
                            name = %s, workshop_link = %s, team_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    update_values = [
                        record.get('name'),
                        record.get('workshop_link'),
                        record.get('team_id')
                    ] + match_values
                    cursor.execute(update_query, tuple(update_values))
                    updated += cursor.rowcount
            
            elif mode == 'upsert':
                if exists:
                    where_clause = ' AND '.join(match_conditions)
                    update_query = f"""
                        UPDATE aws_team_building SET
                            name = %s, workshop_link = %s, team_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    update_values = [
                        record.get('name'),
                        record.get('workshop_link'),
                        record.get('team_id')
                    ] + match_values
                    cursor.execute(update_query, tuple(update_values))
                    updated += cursor.rowcount
                else:
                    insert_query = """
                        INSERT INTO aws_team_building (workshop_name, email, name, workshop_link, team_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record.get('workshop_name'),
                        record.get('email'),
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


def bulk_upsert_advanced_verification(records: list, mode: str = 'upsert', match_fields: list = None):
    """Advanced bulk upsert for Verification"""
    if not records:
        return {"inserted": 0, "updated": 0}
    
    if match_fields is None:
        match_fields = ['workshop_name', 'email']
    
    conn = None
    inserted = 0
    updated = 0
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        for record in records:
            match_conditions, match_values = build_match_query('verification', match_fields, record)
            
            if not match_conditions:
                continue
            
            check_query = f"SELECT workshop_name, email FROM verification WHERE {' AND '.join(match_conditions)}"
            cursor.execute(check_query, tuple(match_values))
            exists = cursor.fetchone()
            
            if mode == 'create':
                if not exists:
                    insert_query = """
                        INSERT INTO verification (
                            workshop_name, email, name, project_ss, project_valid,
                            blog, blog_valid, team_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record.get('workshop_name'),
                        record.get('email'),
                        record.get('name'),
                        record.get('project_ss'),
                        record.get('project_valid', False),
                        record.get('blog'),
                        record.get('blog_valid', False),
                        record.get('team_id')
                    ))
                    inserted += cursor.rowcount
            
            elif mode == 'update':
                if exists:
                    where_clause = ' AND '.join(match_conditions)
                    update_query = f"""
                        UPDATE verification SET
                            name = %s, project_ss = %s, project_valid = %s,
                            blog = %s, blog_valid = %s, team_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    update_values = [
                        record.get('name'),
                        record.get('project_ss'),
                        record.get('project_valid', False),
                        record.get('blog'),
                        record.get('blog_valid', False),
                        record.get('team_id')
                    ] + match_values
                    cursor.execute(update_query, tuple(update_values))
                    updated += cursor.rowcount
            
            elif mode == 'upsert':
                if exists:
                    where_clause = ' AND '.join(match_conditions)
                    update_query = f"""
                        UPDATE verification SET
                            name = %s, project_ss = %s, project_valid = %s,
                            blog = %s, blog_valid = %s, team_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    update_values = [
                        record.get('name'),
                        record.get('project_ss'),
                        record.get('project_valid', False),
                        record.get('blog'),
                        record.get('blog_valid', False),
                        record.get('team_id')
                    ] + match_values
                    cursor.execute(update_query, tuple(update_values))
                    updated += cursor.rowcount
                else:
                    insert_query = """
                        INSERT INTO verification (
                            workshop_name, email, name, project_ss, project_valid,
                            blog, blog_valid, team_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        record.get('workshop_name'),
                        record.get('email'),
                        record.get('name'),
                        record.get('project_ss'),
                        record.get('project_valid', False),
                        record.get('blog'),
                        record.get('blog_valid', False),
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

