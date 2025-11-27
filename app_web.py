"""
Flask Web Application for AWS AI for Bharat Tracking System
"""
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, send_from_directory, session, stream_with_context, Response
from datetime import datetime, timedelta
from functools import wraps
import json
import os
import uuid
from werkzeug.utils import secure_filename
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from database import (
    db_manager, UserPII, FormResponse, AWSTeamBuilding,
    ProjectSubmission, Verification, MasterLogs,
    RBACUser, RBACPermission, RBACUserPermission
)
from import_utils import parse_master_workbook, parse_user_pii_workbook
from database_advanced import (
    bulk_upsert_advanced_user_pii,
    bulk_upsert_advanced_form_response,
    bulk_upsert_advanced_project_submission,
    bulk_upsert_advanced_aws_team_building,
    bulk_upsert_advanced_verification
)
from google_sheets_utils import GoogleSheetsExporter

app = Flask(__name__)
app.secret_key = 'aws-ai-bharat-secret-key-change-in-production'

# Upload configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# Initialize database connection on startup
@app.before_request
def initialize_database():
    try:
        if not db_manager.pool:
            db_manager.create_pool()
    except Exception as e:
        print(f"Warning: Database connection issue: {e}")

# Reload user permissions in session if user is logged in
@app.before_request
def load_user_permissions():
    """Reload user permissions in session on each request"""
    if 'user_id' in session and 'user_routes' not in session:
        user = RBACUser.get_by_id(session['user_id'])
        if user:
            if user.get('is_admin'):
                session['user_routes'] = [p['route_name'] if isinstance(p, dict) else p[0] for p in RBACPermission.get_all()]
            else:
                session['user_routes'] = RBACUserPermission.get_user_permission_routes(session['user_id'])

# ============================================
# RBAC Helper Functions
# ============================================

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorator to require admin access"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        user = RBACUser.get_by_id(session['user_id'])
        if not user or not user.get('is_admin'):
            flash('Admin access required', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def permission_required(route_name):
    """Decorator factory to require specific permission"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                return redirect(url_for('login'))
            user_id = session['user_id']
            if not RBACUserPermission.has_permission(user_id, route_name):
                flash('You do not have permission to access this page', 'error')
                return redirect(url_for('index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator


# ============================================
# Routes - Authentication
# ============================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    """User login"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        if not username or not password:
            flash('Please enter both username and password', 'error')
            return render_template('login.html')
        
        user = RBACUser.get_by_username(username)
        if not user:
            flash('Invalid username or password', 'error')
            return render_template('login.html')
        
        if not user.get('is_active'):
            flash('Your account is inactive. Please contact administrator.', 'error')
            return render_template('login.html')
        
        if not RBACUser.verify_password(user['password_hash'], password):
            flash('Invalid username or password', 'error')
            return render_template('login.html')
        
        # Login successful
        session['user_id'] = user['user_id']
        session['username'] = user['username']
        session['is_admin'] = user.get('is_admin', False)
        session['full_name'] = user.get('full_name', user['username'])
        
        # Load user permissions into session
        if user.get('is_admin'):
            # Admin has all permissions
            session['user_routes'] = [p['route_name'] if isinstance(p, dict) else p[0] for p in RBACPermission.get_all()]
        else:
            session['user_routes'] = RBACUserPermission.get_user_permission_routes(user['user_id'])
        
        # Update last login
        RBACUser.update_last_login(user['user_id'])
        
        flash(f'Welcome back, {session["full_name"]}!', 'success')
        return redirect(url_for('index'))
    
    # If already logged in, redirect to dashboard
    if 'user_id' in session:
        return redirect(url_for('index'))
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """User logout"""
    session.clear()
    flash('You have been logged out successfully', 'success')
    return redirect(url_for('login'))

# ============================================
# Routes - Dashboard
# ============================================
@app.route('/')
@login_required
@permission_required('index')
def index():
    """Main dashboard"""
    try:
        # Get recent activity logs
        recent_logs = MasterLogs.get_all(limit=10)
        
        # Get statistics using SQL COUNT queries for accuracy
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Count total registrations (from user_pii table)
        cursor.execute("SELECT COUNT(*) FROM user_pii")
        total_registration = cursor.fetchone()[0]
        
        # Count total form submissions (from form_response table)
        cursor.execute("SELECT COUNT(*) FROM form_response")
        total_form_submission = cursor.fetchone()[0]
        
        # Count total blog submissions (from project_submission table)
        cursor.execute("SELECT COUNT(*) FROM project_submission")
        total_blog_submission = cursor.fetchone()[0]
        
        db_manager.return_connection(conn)
        
        stats = {
            'total_registration': total_registration,
            'total_form_submission': total_form_submission,
            'total_blog_submission': total_blog_submission
        }
        
        return render_template('dashboard.html', recent_logs=recent_logs, stats=stats)
    except Exception as e:
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template('dashboard.html', recent_logs=[], stats={})


@app.route('/api/dashboard/demographics')
@login_required
@permission_required('index')
def get_demographics():
    """Get demographic statistics for dashboard"""
    try:
        print("=== Demographics API Called ===")
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Gender distribution
        cursor.execute("""
            SELECT gender, COUNT(*) as count 
            FROM user_pii 
            WHERE gender IS NOT NULL AND gender != ''
            GROUP BY gender 
            ORDER BY count DESC
        """)
        gender_data = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"Gender data: {gender_data}")
        
        # Occupation distribution
        cursor.execute("""
            SELECT occupation, COUNT(*) as count 
            FROM user_pii 
            WHERE occupation IS NOT NULL AND occupation != ''
            GROUP BY occupation 
            ORDER BY count DESC
            LIMIT 20
        """)
        occupation_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        # State distribution
        cursor.execute("""
            SELECT state, COUNT(*) as count 
            FROM user_pii 
            WHERE state IS NOT NULL AND state != ''
            GROUP BY state 
            ORDER BY count DESC
        """)
        state_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        # City distribution (top 20)
        cursor.execute("""
            SELECT city, COUNT(*) as count 
            FROM user_pii 
            WHERE city IS NOT NULL AND city != ''
            GROUP BY city 
            ORDER BY count DESC
            LIMIT 20
        """)
        city_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Age distribution (calculated from date_of_birth)
        # Use a subquery to calculate age groups, then order by the group name
        cursor.execute("""
            SELECT age_group, count
            FROM (
                SELECT 
                    CASE 
                        WHEN date_of_birth IS NULL THEN 'Not Specified'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) < 18 THEN 'Under 18'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 18 AND 25 THEN '18-25'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 26 AND 30 THEN '26-30'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 31 AND 35 THEN '31-35'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 36 AND 40 THEN '36-40'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 41 AND 50 THEN '41-50'
                        ELSE 'Above 50'
                    END as age_group,
                    COUNT(*) as count
                FROM user_pii
                GROUP BY 
                    CASE 
                        WHEN date_of_birth IS NULL THEN 'Not Specified'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) < 18 THEN 'Under 18'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 18 AND 25 THEN '18-25'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 26 AND 30 THEN '26-30'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 31 AND 35 THEN '31-35'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 36 AND 40 THEN '36-40'
                        WHEN EXTRACT(YEAR FROM AGE(date_of_birth)) BETWEEN 41 AND 50 THEN '41-50'
                        ELSE 'Above 50'
                    END
            ) as age_groups
            ORDER BY 
                CASE age_group
                    WHEN 'Under 18' THEN 1
                    WHEN '18-25' THEN 2
                    WHEN '26-30' THEN 3
                    WHEN '31-35' THEN 4
                    WHEN '36-40' THEN 5
                    WHEN '41-50' THEN 6
                    WHEN 'Above 50' THEN 7
                    WHEN 'Not Specified' THEN 8
                    ELSE 9
                END
        """)
        age_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Workshop slot bookings (from form_response)
        cursor.execute("""
            SELECT form_name, COUNT(*) as count 
            FROM form_response 
            GROUP BY form_name 
            ORDER BY form_name
        """)
        workshop_bookings = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Time slot distribution
        cursor.execute("""
            SELECT slot, COUNT(*) as count
            FROM (
                SELECT 
                    CASE 
                        WHEN time_slot_range IS NOT NULL AND time_slot_range != '' THEN time_slot_range
                        WHEN time_slot IS NOT NULL THEN TO_CHAR(time_slot::TIMESTAMP, 'YYYY-MM-DD HH24:MI')
                        ELSE 'No Time Slot'
                    END as slot
                FROM form_response
            ) as slots
            GROUP BY slot
            ORDER BY count DESC
            LIMIT 15
        """)
        time_slot_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Designation distribution (top 15)
        cursor.execute("""
            SELECT designation, COUNT(*) as count 
            FROM user_pii 
            WHERE designation IS NOT NULL AND designation != ''
            GROUP BY designation 
            ORDER BY count DESC
            LIMIT 15
        """)
        designation_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Registration trend (by date)
        cursor.execute("""
            SELECT 
                TO_CHAR(registration_date_time, 'YYYY-MM-DD') as date,
                COUNT(*) as count
            FROM user_pii
            WHERE registration_date_time IS NOT NULL
            GROUP BY date
            ORDER BY date DESC
            LIMIT 60
        """)
        registration_trend = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Occupation breakdown by workshop and time slot
        cursor.execute("""
            SELECT 
                fr.form_name as workshop,
                COALESCE(
                    NULLIF(fr.time_slot_range, ''),
                    CASE 
                        WHEN fr.time_slot IS NOT NULL 
                        THEN TO_CHAR(fr.time_slot::TIMESTAMP, 'YYYY-MM-DD HH24:MI')
                        ELSE 'No Time Slot'
                    END
                ) as time_slot,
                COALESCE(NULLIF(u.occupation, ''), 'Unknown') as occupation,
                COUNT(*) as count
            FROM form_response fr
            LEFT JOIN user_pii u ON fr.email = u.email
            WHERE fr.form_name LIKE 'Workshop %'
            GROUP BY fr.form_name, 
                     COALESCE(
                         NULLIF(fr.time_slot_range, ''),
                         CASE 
                             WHEN fr.time_slot IS NOT NULL 
                             THEN TO_CHAR(fr.time_slot::TIMESTAMP, 'YYYY-MM-DD HH24:MI')
                             ELSE 'No Time Slot'
                         END
                     ),
                     COALESCE(NULLIF(u.occupation, ''), 'Unknown')
            ORDER BY fr.form_name, time_slot, occupation
        """)
        workshop_occupation_data = []
        for row in cursor.fetchall():
            workshop_occupation_data.append({
                'workshop': row[0],
                'time_slot': row[1],
                'occupation': row[2],
                'count': row[3]
            })
        
        db_manager.return_connection(conn)
        
        result = {
            'success': True,
            'gender': gender_data,
            'occupation': occupation_data,
            'state': state_data,
            'city': city_data,
            'age': age_data,
            'workshop_bookings': workshop_bookings,
            'time_slots': time_slot_data,
            'designation': designation_data,
            'registration_trend': registration_trend,
            'workshop_occupation_breakdown': workshop_occupation_data
        }
        
        print(f"Demographics result: {result}")
        return jsonify(result)
    
    except Exception as e:
        print(f"ERROR in demographics endpoint: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============================================
# Routes - User PII
# ============================================
@app.route('/users')
@login_required
@permission_required('users_list')
def users_list():
    """List all users"""
    try:
        users = UserPII.list_all()
        return render_template('users_list.html', users=users)
    except Exception as e:
        flash(f'Error loading users: {str(e)}', 'error')
        return render_template('users_list.html', users=[])


@app.route('/users/create', methods=['GET', 'POST'])
@login_required
@permission_required('user_create')
def user_create():
    """Create a new user"""
    if request.method == 'POST':
        try:
            data = request.form
            UserPII.create(
                email=data.get('email'),
                name=data.get('name'),
                phone_number=data.get('phone_number') or None,
                gender=data.get('gender') or None,
                country=data.get('country') or None,
                state=data.get('state') or None,
                city=data.get('city') or None,
                date_of_birth=data.get('date_of_birth') or None,
                designation=data.get('designation') or None,
                class_stream=data.get('class_stream') or None,
                degree_passout_year=int(data.get('degree_passout_year')) if data.get('degree_passout_year') else None,
                occupation=data.get('occupation') or None,
                linkedin=data.get('linkedin') or None,
                participated_in_academy_1_0=data.get('participated_in_academy_1_0') == 'on'
            )
            flash('User created successfully!', 'success')
            return redirect(url_for('users_list'))
        except Exception as e:
            flash(f'Error creating user: {str(e)}', 'error')
    return render_template('user_form.html', user=None)


@app.route('/users/<email>')
@login_required
@permission_required('users_list')
def user_view(email):
    """View user details"""
    try:
        user = UserPII.get(email)
        if not user:
            flash('User not found', 'error')
            return redirect(url_for('users_list'))
        
        # Get activity logs for this user
        logs = MasterLogs.get_by_record('user_pii', email)
        
        return render_template('user_view.html', user=user, logs=logs)
    except Exception as e:
        flash(f'Error loading user: {str(e)}', 'error')
        return redirect(url_for('users_list'))


@app.route('/users/<email>/edit', methods=['GET', 'POST'])
@login_required
@permission_required('user_create')
def user_edit(email):
    """Edit user"""
    user = UserPII.get(email)
    if not user:
        flash('User not found', 'error')
        return redirect(url_for('users_list'))
    
    if request.method == 'POST':
        try:
            data = request.form
            UserPII.update(
                email,
                phone_number=data.get('phone_number') or None,
                gender=data.get('gender') or None,
                country=data.get('country') or None,
                state=data.get('state') or None,
                city=data.get('city') or None,
                date_of_birth=data.get('date_of_birth') or None,
                designation=data.get('designation') or None,
                class_stream=data.get('class_stream') or None,
                degree_passout_year=int(data.get('degree_passout_year')) if data.get('degree_passout_year') else None,
                occupation=data.get('occupation') or None,
                linkedin=data.get('linkedin') or None,
                participated_in_academy_1_0=data.get('participated_in_academy_1_0') == 'on'
            )
            flash('User updated successfully!', 'success')
            return redirect(url_for('user_view', email=email))
        except Exception as e:
            flash(f'Error updating user: {str(e)}', 'error')
    
    return render_template('user_form.html', user=user)


# ============================================
# Routes - Form Response (Removed - use Workshops page instead)
# ============================================

@app.route('/workshops')
@login_required
@permission_required('workshops_view')
def workshops_view():
    """Workshops view with tabs for each workshop"""
    return render_template('workshops_view.html')


@app.route('/api/workshops/<int:workshop_num>/data')
def get_workshop_data(workshop_num):
    """Get form response data for a specific workshop, grouped by time slot"""
    try:
        workshop_name = f'Workshop {workshop_num}'
        
        # Query form responses for this workshop
        query = """
            SELECT fr.email, fr.form_name, fr.name, fr.time_slot, fr.time_slot_range, fr.created_at,
                   u.name as user_name, u.phone_number, u.designation, u.occupation, u.linkedin
            FROM form_response fr
            LEFT JOIN user_pii u ON fr.email = u.email
            WHERE fr.form_name = %s
            ORDER BY fr.time_slot, fr.created_at
        """
        
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (workshop_name,))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        db_manager.return_connection(conn)
        
        # Convert to list of dicts
        form_responses = []
        for row in rows:
            form_responses.append(dict(zip(columns, row)))
        
        # Group by time_slot_range (original string) or time_slot
        grouped_data = {}
        for response in form_responses:
            # Use time_slot_range if available, otherwise use time_slot
            time_slot_key = response.get('time_slot_range')
            if not time_slot_key:
                time_slot = response.get('time_slot')
                if time_slot:
                    time_slot_key = time_slot.strftime('%Y-%m-%d %H:%M:%S') if isinstance(time_slot, datetime) else str(time_slot)
                else:
                    time_slot_key = 'No Time Slot'
            
            if time_slot_key not in grouped_data:
                grouped_data[time_slot_key] = []
            grouped_data[time_slot_key].append(response)
        
        # Calculate occupation breakdown for each time slot
        occupation_breakdown = {}
        for time_slot_key, responses in grouped_data.items():
            occupation_counts = {}
            for response in responses:
                occupation = response.get('occupation') or 'Not Specified'
                occupation_counts[occupation] = occupation_counts.get(occupation, 0) + 1
            occupation_breakdown[time_slot_key] = occupation_counts
        
        # Convert datetime objects to strings for JSON
        for time_slot, responses in grouped_data.items():
            for response in responses:
                if response.get('time_slot') and isinstance(response['time_slot'], datetime):
                    response['time_slot'] = response['time_slot'].strftime('%Y-%m-%d %H:%M:%S')
                if response.get('created_at') and isinstance(response['created_at'], datetime):
                    response['created_at'] = response['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'success': True,
            'workshop_name': workshop_name,
            'total_responses': len(form_responses),
            'time_slots': grouped_data,
            'occupation_breakdown': occupation_breakdown
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/workshops/<int:workshop_num>/export')
def export_workshop_data(workshop_num):
    """Export workshop data as CSV"""
    try:
        from flask import Response
        import csv
        import io
        from datetime import datetime
        
        workshop_name = f'Workshop {workshop_num}'
        time_slot_filter = request.args.get('time_slot', None)
        
        # Query form responses
        if time_slot_filter and time_slot_filter != 'No Time Slot':
            query = """
                SELECT fr.email, fr.form_name, fr.name, fr.time_slot, fr.created_at,
                       u.name as user_name, u.phone_number, u.designation, u.occupation, u.linkedin
                FROM form_response fr
                LEFT JOIN user_pii u ON fr.email = u.email
                WHERE fr.form_name = %s AND fr.time_slot::text = %s
                ORDER BY fr.time_slot, fr.created_at
            """
            params = (workshop_name, time_slot_filter)
        else:
            query = """
                SELECT fr.email, fr.form_name, fr.name, fr.time_slot, fr.created_at,
                       u.name as user_name, u.phone_number, u.designation, u.occupation, u.linkedin
                FROM form_response fr
                LEFT JOIN user_pii u ON fr.email = u.email
                WHERE fr.form_name = %s
                ORDER BY fr.time_slot, fr.created_at
            """
            params = (workshop_name,)
        
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        db_manager.return_connection(conn)
        
        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(columns)
        
        # Write data
        for row in rows:
            # Convert datetime objects to strings
            row_data = []
            for val in row:
                if isinstance(val, datetime):
                    row_data.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    row_data.append(str(val) if val is not None else '')
            writer.writerow(row_data)
        
        # Create response
        filename = f'workshop_{workshop_num}'
        if time_slot_filter and time_slot_filter != 'No Time Slot':
            # Sanitize time_slot for filename
            safe_time_slot = time_slot_filter.replace(' ', '_').replace(':', '-')[:20]
            filename += f'_{safe_time_slot}'
        filename += '.csv'
        
        response = Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename={filename}'
            }
        )
        
        return response
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================
# Routes - AWS Team Building
# ============================================
@app.route('/team-building')
@login_required
@permission_required('team_building_list')
def team_building_list():
    """List all AWS team building records"""
    try:
        records = AWSTeamBuilding.list_all()
        return render_template('team_building_list.html', records=records)
    except Exception as e:
        flash(f'Error loading records: {str(e)}', 'error')
        return render_template('team_building_list.html', records=[])


@app.route('/team-building/create', methods=['GET', 'POST'])
@login_required
@permission_required('team_building_create')
def team_building_create():
    """Create AWS team building record"""
    if request.method == 'POST':
        try:
            data = request.form
            AWSTeamBuilding.create(
                workshop_name=data.get('workshop_name'),
                email=data.get('email'),
                name=data.get('name'),
                workshop_link=data.get('workshop_link') or None,
                team_id=data.get('team_id') or None
            )
            flash('Team building record created successfully!', 'success')
            return redirect(url_for('team_building_list'))
        except Exception as e:
            flash(f'Error creating record: {str(e)}', 'error')
    
    users = UserPII.list_all()
    return render_template('team_building_form.html', record=None, users=users)


# ============================================
# Routes - Blog Submission (formerly Project Submission)
# ============================================
def scrape_blog_metrics(blog_url):
    """
    Scrape likes and comments count from a blog URL using Selenium
    Returns: (likes: int, comments: int, error: str or None, is_404: bool)
    """
    likes = 0
    comments = 0
    error = None
    is_404 = False
    
    print(f"[DEBUG] scrape_blog_metrics called for: {blog_url}")
    
    try:
        # Use Selenium for builder.aws.com URLs (always JS-rendered)
        if 'builder.aws.com' in blog_url:
            print(f"[DEBUG] Detected builder.aws.com URL, using Selenium")
            try:
                from selenium import webdriver
                from selenium.webdriver.chrome.options import Options
                from selenium.webdriver.chrome.service import Service
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                from webdriver_manager.chrome import ChromeDriverManager
                import time
                
                print(f"[DEBUG] Selenium imports successful")
                
                chrome_options = Options()
                chrome_options.add_argument('--headless=new')  # Use new headless mode (faster)
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--disable-extensions')
                chrome_options.add_argument('--disable-plugins')
                chrome_options.add_argument('--disable-background-timer-throttling')
                chrome_options.add_argument('--disable-backgrounding-occluded-windows')
                chrome_options.add_argument('--disable-renderer-backgrounding')
                chrome_options.add_argument('--disable-features=TranslateUI')
                chrome_options.add_argument('--disable-ipc-flooding-protection')
                chrome_options.add_argument('--disable-hang-monitor')
                chrome_options.add_argument('--disable-prompt-on-repost')
                chrome_options.add_argument('--disable-domain-reliability')
                chrome_options.add_argument('--disable-component-update')
                chrome_options.add_argument('--disable-background-networking')
                chrome_options.add_argument('--disable-sync')
                chrome_options.add_argument('--disable-default-apps')
                chrome_options.add_argument('--disable-breakpad')
                chrome_options.add_argument('--disable-client-side-phishing-detection')
                chrome_options.add_argument('--disable-crash-reporter')
                chrome_options.add_argument('--disable-features=AudioServiceOutOfProcess')
                chrome_options.add_argument('--blink-settings=imagesEnabled=false')  # Disable images
                chrome_options.page_load_strategy = 'eager'  # Don't wait for all resources
                
                # Block images and CSS to speed up loading
                prefs = {
                    "profile.managed_default_content_settings.images": 2,  # Block images
                    "profile.default_content_setting_values.notifications": 2,
                    "profile.default_content_settings.popups": 0,
                }
                chrome_options.add_experimental_option("prefs", prefs)
                
                chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
                
                print(f"[DEBUG] Setting up Chrome driver...")
                # Use webdriver-manager to automatically handle ChromeDriver
                driver = None
                try:
                    # Try with webdriver-manager first
                    driver_path = ChromeDriverManager().install()
                    print(f"[DEBUG] ChromeDriverManager returned path: {driver_path}")
                    
                    # Fix: webdriver-manager sometimes returns wrong file, find the actual chromedriver.exe
                    import os
                    driver_dir = os.path.dirname(driver_path)
                    actual_driver = None
                    
                    # Look for chromedriver.exe in the directory (recursively)
                    def find_chromedriver(directory):
                        """Recursively search for chromedriver.exe"""
                        if not os.path.isdir(directory):
                            return None
                        for root, dirs, files in os.walk(directory):
                            for file in files:
                                if file == 'chromedriver.exe':
                                    full_path = os.path.join(root, file)
                                    # Verify it's actually an executable (check file size > 1MB)
                                    try:
                                        if os.path.getsize(full_path) > 1000000:
                                            return full_path
                                    except:
                                        pass
                        return None
                    
                    # Check if ChromeDriver is in a zip file and extract it
                    import zipfile
                    zip_files = [f for f in os.listdir(driver_dir) if f.endswith('.zip')]
                    if zip_files:
                        zip_path = os.path.join(driver_dir, zip_files[0])
                        print(f"[DEBUG] Found zip file: {zip_path}, extracting...")
                        try:
                            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                zip_ref.extractall(driver_dir)
                            print(f"[DEBUG] Extracted ChromeDriver from zip")
                        except Exception as e:
                            print(f"[DEBUG] Failed to extract zip: {e}")
                    
                    # Search in driver directory and parent
                    actual_driver = find_chromedriver(driver_dir)
                    if not actual_driver:
                        parent_dir = os.path.dirname(driver_dir)
                        actual_driver = find_chromedriver(parent_dir)
                    
                    # Use actual driver if found, otherwise try the original path
                    if actual_driver and os.path.exists(actual_driver):
                        driver_path = actual_driver
                        print(f"[DEBUG] Found actual ChromeDriver at: {driver_path}")
                    else:
                        print(f"[DEBUG] Using ChromeDriverManager path: {driver_path}")
                        # If the path doesn't exist or is wrong, try to find it
                        if not os.path.exists(driver_path) or driver_path.endswith('.zip') or 'THIRD_PARTY' in driver_path:
                            # Search more broadly
                            wdm_base = os.path.expanduser('~/.wdm')
                            if os.path.isdir(wdm_base):
                                actual_driver = find_chromedriver(wdm_base)
                                if actual_driver:
                                    driver_path = actual_driver
                                    print(f"[DEBUG] Found ChromeDriver in .wdm: {driver_path}")
                    
                    # Verify the file exists
                    if not os.path.exists(driver_path):
                        raise Exception(f"ChromeDriver not found at {driver_path}")
                    
                    # Check file size (should be > 0)
                    file_size = os.path.getsize(driver_path)
                    print(f"[DEBUG] ChromeDriver file size: {file_size:,} bytes")
                    if file_size < 1000:  # ChromeDriver should be at least 1KB
                        raise Exception(f"ChromeDriver file appears corrupted (size: {file_size} bytes)")
                    
                    service = Service(driver_path)
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                    print(f"[DEBUG] Chrome driver initialized successfully")
                except Exception as e:
                    print(f"[WARNING] webdriver-manager failed: {e}")
                    # Try without webdriver-manager (if ChromeDriver is in PATH)
                    try:
                        print(f"[DEBUG] Trying ChromeDriver from PATH...")
                        driver = webdriver.Chrome(options=chrome_options)
                        print(f"[DEBUG] Chrome driver initialized from PATH")
                    except Exception as e2:
                        print(f"[ERROR] ChromeDriver from PATH also failed: {e2}")
                        # Last resort: try to find ChromeDriver in common locations
                        import shutil
                        chromedriver_path = shutil.which('chromedriver')
                        if chromedriver_path:
                            print(f"[DEBUG] Found chromedriver at: {chromedriver_path}")
                            service = Service(chromedriver_path)
                            driver = webdriver.Chrome(service=service, options=chrome_options)
                            print(f"[DEBUG] Chrome driver initialized from which()")
                        else:
                            raise Exception(f"Could not find ChromeDriver. Please install it manually or ensure it's in PATH. Error: {e2}")
                
                print(f"[DEBUG] Loading page: {blog_url}")
                driver.get(blog_url)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                print(f"[DEBUG] Page loaded, waiting for dynamic content...")
                time.sleep(5)  # Wait longer for dynamic content to load
                print(f"[DEBUG] Dynamic content wait complete")
                
                # Save page source for debugging
                try:
                    with open('debug_page_source.html', 'w', encoding='utf-8') as f:
                        f.write(driver.page_source)
                    print(f"[DEBUG] Saved page source to debug_page_source.html")
                except:
                    pass
                
                # Check for 404 page
                page_title = driver.title.lower()
                page_source = driver.page_source.lower()
                
                # Check for 404 indicators
                if ('404' in page_title or 
                    'not found' in page_title or 
                    '404' in page_source[:2000] or 
                    'page you\'re looking for can\'t be found' in page_source or
                    'the page you\'re looking for can\'t be found' in page_source):
                    is_404 = True
                    error = "404 Not Found"
                    driver.quit()
                    return likes, comments, error, is_404
                
                # Initialize likes and comments to None (not 0) so we can distinguish between "not found" and "found 0"
                likes_found = False
                comments_found = False
                
                # Try to find like and comment elements using Selenium
                print(f"[DEBUG] Searching for like/comment elements on {blog_url}")
                try:
                    # Method 1: Find Like button with exact aria-label and extract from _card-action-text span
                    print(f"[DEBUG] Method 1: Searching for 'Like this article' button...")
                    like_buttons = driver.find_elements(By.XPATH, "//button[@aria-label='Like this article']")
                    if not like_buttons:
                        # Fallback to contains
                        like_buttons = driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Like this article')]")
                    
                    print(f"[DEBUG] Found {len(like_buttons)} Like button(s)")
                    
                    # Process each button to find the correct one
                    for btn in like_buttons:
                        try:
                            aria_label = btn.get_attribute('aria-label')
                            print(f"[DEBUG] Like button - aria-label: '{aria_label}'")
                            
                            # Priority 1: Find span with class containing '_card-action-text' (the specific span with the count)
                            action_text_spans = btn.find_elements(By.CSS_SELECTOR, "span[class*='_card-action-text']")
                            print(f"[DEBUG] Found {len(action_text_spans)} span(s) with '_card-action-text' class")
                            
                            for span in action_text_spans:
                                span_text = span.text.strip()
                                span_class = span.get_attribute('class') or ''
                                print(f"[DEBUG] Like action-text span - text: '{span_text}', class: '{span_class}'")
                                
                                # Extract number from this span (even if it's 0)
                                if span_text.isdigit():
                                    likes = int(span_text)
                                    likes_found = True
                                    print(f"[DEBUG] ✓ Extracted likes from _card-action-text span: {likes}")
                                    break
                            
                            # Priority 2: If not found in _card-action-text, check all spans and look for numeric text
                            if not likes_found:
                                spans = btn.find_elements(By.TAG_NAME, "span")
                                print(f"[DEBUG] Checking all {len(spans)} span(s) in Like button")
                                for span in spans:
                                    span_text = span.text.strip()
                                    span_class = span.get_attribute('class') or ''
                                    print(f"[DEBUG] Like span - text: '{span_text}', class: '{span_class}'")
                                    
                                    # Only accept if it's a pure number (not part of a larger string)
                                    if span_text.isdigit():
                                        likes = int(span_text)
                                        likes_found = True
                                        print(f"[DEBUG] ✓ Extracted likes from span: {likes}")
                                        break
                            
                            # If we found a value (including 0), break
                            if likes_found:
                                print(f"[DEBUG] Final likes value: {likes}")
                                break
                        except Exception as e:
                            print(f"[DEBUG] Error processing Like button: {e}")
                            import traceback
                            traceback.print_exc()
                            continue
                    
                    # Method 2: Find Comment button with exact aria-label and extract from _card-action-text span
                    print(f"[DEBUG] Method 2: Searching for 'Comment on this article' button...")
                    comment_buttons = driver.find_elements(By.XPATH, "//button[@aria-label='Comment on this article']")
                    if not comment_buttons:
                        # Fallback to contains
                        comment_buttons = driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Comment on this article')]")
                    
                    print(f"[DEBUG] Found {len(comment_buttons)} Comment button(s)")
                    
                    # Process each button to find the correct one
                    for btn in comment_buttons:
                        try:
                            aria_label = btn.get_attribute('aria-label')
                            print(f"[DEBUG] Comment button - aria-label: '{aria_label}'")
                            
                            # Priority 1: Find span with class containing '_card-action-text' (the specific span with the count)
                            action_text_spans = btn.find_elements(By.CSS_SELECTOR, "span[class*='_card-action-text']")
                            print(f"[DEBUG] Found {len(action_text_spans)} span(s) with '_card-action-text' class")
                            
                            for span in action_text_spans:
                                span_text = span.text.strip()
                                span_class = span.get_attribute('class') or ''
                                print(f"[DEBUG] Comment action-text span - text: '{span_text}', class: '{span_class}'")
                                
                                # Extract number from this span (even if it's 0)
                                if span_text.isdigit():
                                    comments = int(span_text)
                                    comments_found = True
                                    print(f"[DEBUG] ✓ Extracted comments from _card-action-text span: {comments}")
                                    break
                            
                            # Priority 2: If not found in _card-action-text, check all spans and look for numeric text
                            if not comments_found:
                                spans = btn.find_elements(By.TAG_NAME, "span")
                                print(f"[DEBUG] Checking all {len(spans)} span(s) in Comment button")
                                for span in spans:
                                    span_text = span.text.strip()
                                    span_class = span.get_attribute('class') or ''
                                    print(f"[DEBUG] Comment span - text: '{span_text}', class: '{span_class}'")
                                    
                                    # Only accept if it's a pure number (not part of a larger string)
                                    if span_text.isdigit():
                                        comments = int(span_text)
                                        comments_found = True
                                        print(f"[DEBUG] ✓ Extracted comments from span: {comments}")
                                        break
                            
                            # If we found a value (including 0), break
                            if comments_found:
                                print(f"[DEBUG] Final comments value: {comments}")
                                break
                        except Exception as e:
                            print(f"[DEBUG] Error processing Comment button: {e}")
                            import traceback
                            traceback.print_exc()
                            continue
                    
                    # If we didn't find values, keep them as 0 (default)
                    if not likes_found:
                        print(f"[DEBUG] Like button not found or no numeric value extracted, keeping likes=0")
                    if not comments_found:
                        print(f"[DEBUG] Comment button not found or no numeric value extracted, keeping comments=0")
                            
                except Exception as e:
                    print(f"[ERROR] Error finding elements: {e}")
                    import traceback
                    traceback.print_exc()
                
                # Final validation: ensure we only accept values from the correct span
                print(f"[DEBUG] Final validation - Likes found: {likes_found}, Comments found: {comments_found}")
                print(f"[DEBUG] Final results before driver.quit(): Likes={likes}, Comments={comments}")
                
                # If we didn't find values using Selenium, don't try BeautifulSoup (it's unreliable)
                # Keep the default 0 values
                driver.quit()
                print(f"[DEBUG] Driver closed")
                
            except ImportError as e:
                error = f"Selenium not available. Install with: pip install selenium webdriver-manager. Error: {str(e)}"
                print(f"[ERROR] {error}")
                import traceback
                traceback.print_exc()
            except Exception as e:
                error = f"Selenium error: {str(e)}"
                print(f"[ERROR] Selenium exception: {error}")
                import traceback
                traceback.print_exc()
                try:
                    driver.quit()
                except:
                    pass
        else:
            # For community.aws or other sites, use regular requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
            
            response = requests.get(blog_url, headers=headers, timeout=15)
            
            # Check for 404
            if response.status_code == 404:
                is_404 = True
                error = "404 Not Found"
                return likes, comments, error, is_404
            
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check for 404 in content
            page_text = soup.get_text().lower()
            if '404' in page_text[:1000] or 'not found' in page_text[:1000]:
                is_404 = True
                error = "404 Not Found"
                return likes, comments, error, is_404
            
            # Parse likes and comments - prioritize _card-action-text span
            like_button = soup.find('button', {'aria-label': re.compile(r'Like this article', re.I)})
            if like_button:
                # First try to find span with _card-action-text class
                action_text_span = like_button.find('span', class_=re.compile(r'_card-action-text'))
                if action_text_span:
                    text = action_text_span.get_text(strip=True)
                    if text.isdigit():
                        likes = int(text)
                else:
                    # Fallback to all spans
                    spans = like_button.find_all('span')
                    for span in spans:
                        text = span.get_text(strip=True)
                        if text.isdigit():
                            likes = int(text)
                            break
            
            comment_button = soup.find('button', {'aria-label': re.compile(r'Comment on this article', re.I)})
            if comment_button:
                # First try to find span with _card-action-text class
                action_text_span = comment_button.find('span', class_=re.compile(r'_card-action-text'))
                if action_text_span:
                    text = action_text_span.get_text(strip=True)
                    if text.isdigit():
                        comments = int(text)
                else:
                    # Fallback to all spans
                    spans = comment_button.find_all('span')
                    for span in spans:
                        text = span.get_text(strip=True)
                        if text.isdigit():
                            comments = int(text)
                            break
        
    except requests.exceptions.Timeout:
        error = "Request timeout"
    except requests.exceptions.ConnectionError:
        error = "Connection error"
    except requests.exceptions.RequestException as e:
        if '404' in str(e) or e.response and e.response.status_code == 404:
            is_404 = True
            error = "404 Not Found"
        else:
            error = f"Request error: {str(e)}"
    except Exception as e:
        error = f"Error scraping metrics: {str(e)}"
    
    return likes, comments, error, is_404


@app.route('/api/blog-submissions/statistics')
@login_required
@permission_required('blog_submissions_list')
def blog_submissions_statistics():
    """Get blog submission statistics per workshop"""
    try:
        query = """
            SELECT 
                workshop_name,
                COUNT(*) as total_count,
                COUNT(CASE WHEN valid = true THEN 1 END) as valid_count,
                COUNT(CASE WHEN valid = false OR valid IS NULL THEN 1 END) as invalid_count
            FROM project_submission
            GROUP BY workshop_name
            ORDER BY workshop_name
        """
        results = db_manager.execute_query(query)
        
        statistics = []
        total_all = 0
        valid_all = 0
        invalid_all = 0
        
        for row in results:
            stats = {
                'workshop_name': row.get('workshop_name', 'Unknown'),
                'total': row.get('total_count', 0),
                'valid': row.get('valid_count', 0),
                'invalid': row.get('invalid_count', 0)
            }
            statistics.append(stats)
            total_all += stats['total']
            valid_all += stats['valid']
            invalid_all += stats['invalid']
        
        return jsonify({
            'success': True,
            'statistics': statistics,
            'totals': {
                'total': total_all,
                'valid': valid_all,
                'invalid': invalid_all
            }
        })
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/blog-submissions')
@login_required
@permission_required('blog_submissions_list')
def blog_submissions_list():
    """List all blog submissions"""
    try:
        submissions = ProjectSubmission.list_all()
        return render_template('project_submissions_list.html', submissions=submissions)
    except Exception as e:
        flash(f'Error loading submissions: {str(e)}', 'error')
        return render_template('project_submissions_list.html', submissions=[])


@app.route('/blog-submissions/create', methods=['GET', 'POST'])
@login_required
@permission_required('blog_submission_create')
def blog_submission_create():
    """Create blog submission"""
    if request.method == 'POST':
        try:
            data = request.form
            ProjectSubmission.create(
                workshop_name=data.get('workshop_name'),
                email=data.get('email'),
                name=data.get('name'),
                project_link=data.get('project_link') or None,
                valid=data.get('valid') == 'on',
                team_id=data.get('team_id') or None
            )
            flash('Blog submission created successfully!', 'success')
            return redirect(url_for('blog_submissions_list'))
        except Exception as e:
            flash(f'Error creating submission: {str(e)}', 'error')
    
    users = UserPII.list_all()
    return render_template('project_submission_form.html', submission=None, users=users)


def validate_single_submission(submission):
    """Validate a single blog submission (for parallel processing)
    Always re-verifies and updates likes/comments even if submission was already valid
    """
    link = submission.get('project_link')
    if not link:
        return None
    
    is_valid = False
    reason = "Unknown Error"
    likes = 0
    comments = 0
    
    try:
        # Check domain
        if 'community.aws' in link or 'builder.aws.com' in link:
            print(f"[DEBUG] Validating link: {link}")
            # Use scrape_blog_metrics which handles Selenium and 404 detection
            scraped_likes, scraped_comments, scrape_error, is_404 = scrape_blog_metrics(link)
            
            print(f"[DEBUG] Scraped results - Likes: {scraped_likes}, Comments: {scraped_comments}, Error: {scrape_error}, Is_404: {is_404}")
            
            # Check for 404 first
            if is_404 or (scrape_error and "404" in scrape_error):
                is_valid = False
                reason = "404 Not Found"
                likes = 0
                comments = 0
            else:
                # Page is valid, use scraped metrics (always update likes/comments)
                is_valid = True
                likes = scraped_likes
                comments = scraped_comments
                
                if scrape_error:
                    reason = f"Verified but {scrape_error}"
                else:
                    reason = "Verified"
                
                print(f"[DEBUG] Setting - Valid: {is_valid}, Likes: {likes}, Comments: {comments}, Reason: {reason}")
        else:
            reason = "Invalid Domain"
    except Exception as e:
        print(f"[ERROR] Error validating link {link}: {e}")
        import traceback
        traceback.print_exc()
        reason = f"System Error: {str(e)}"
    
    # Always update submission (even if it was already valid) to refresh likes/comments
    try:
        ProjectSubmission.update(
            submission['workshop_name'],
            submission['email'],
            valid=is_valid,
            validation_reason=reason,
            likes=likes,
            comments=comments
        )
        print(f"[DEBUG] Updated submission - {submission['email']}: Valid={is_valid}, Likes={likes}, Comments={comments}")
    except Exception as e:
        print(f"[ERROR] Failed to update submission: {e}")
        import traceback
        traceback.print_exc()
    
    return {
        'workshop_name': submission['workshop_name'],
        'email': submission['email'],
        'link': link,
        'valid': is_valid,
        'reason': reason,
        'likes': likes,
        'comments': comments
    }


@app.route('/blog-submissions/validate', methods=['POST'])
@login_required
@permission_required('blog_submission_create')
def blog_submissions_validate():
    """Validate blog submissions with parallel processing - re-verifies ALL submissions to update likes/comments"""
    try:
        # Get ALL submissions (not just invalid ones) to re-verify and update likes/comments
        submissions = ProjectSubmission.list_all()
        submissions_to_validate = [s for s in submissions if s.get('project_link')]
        
        if not submissions_to_validate:
            flash('No submissions with links to validate.', 'info')
            return redirect(url_for('blog_submissions_list'))
        
        validated_count = 0
        failed_count = 0
        updated_count = 0
        
        # Use ThreadPoolExecutor with 20 workers for parallel processing (increased for faster validation)
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Submit all validation tasks
            future_to_submission = {
                executor.submit(validate_single_submission, submission): submission 
                for submission in submissions_to_validate
            }
            
            # Process results as they complete
            for future in as_completed(future_to_submission):
                try:
                    result = future.result()
                    if result:
                        if result['valid']:
                            validated_count += 1
                            # Check if likes or comments were updated
                            if result.get('likes', 0) > 0 or result.get('comments', 0) > 0:
                                updated_count += 1
                        else:
                            failed_count += 1
                except Exception as e:
                    print(f"[ERROR] Error processing submission: {e}")
                    import traceback
                    traceback.print_exc()
                    failed_count += 1
        
        if validated_count > 0:
            flash(f'Successfully validated {validated_count} submissions. Updated likes/comments for {updated_count} submissions.', 'success')
        
        if failed_count > 0:
            flash(f'Could not validate {failed_count} submissions. Please check them manually.', 'warning')
            
        if validated_count == 0 and failed_count == 0:
             flash('No submissions were processed.', 'info')

        return redirect(url_for('blog_submissions_list'))
    except Exception as e:
        flash(f'Error during validation: {str(e)}', 'error')
        return redirect(url_for('blog_submissions_list'))


@app.route('/api/blog-submissions/validate-stream')
@login_required
@permission_required('blog_submission_create')
def blog_submissions_validate_stream():
    """Stream validation progress with parallel processing - re-verifies ALL submissions to update likes/comments"""
    def generate():
        try:
            # Get ALL submissions (not just invalid ones) to re-verify and update likes/comments
            submissions = ProjectSubmission.list_all()
            submissions_to_validate = [s for s in submissions if s.get('project_link')]
            total_count = len(submissions_to_validate)
            
            if total_count == 0:
                yield json.dumps({'current': 0, 'total': 0, 'status': 'No submissions with links to validate'}) + '\n'
                return

            validated_count = 0
            failed_count = 0
            processed_count = 0
            updated_count = 0
            
            # Use ThreadPoolExecutor with 10 workers for parallel processing
            with ThreadPoolExecutor(max_workers=10) as executor:
                # Submit all validation tasks
                future_to_submission = {
                    executor.submit(validate_single_submission, submission): submission 
                    for submission in submissions_to_validate
                }
                
                # Process results as they complete
                for future in as_completed(future_to_submission):
                    try:
                        result = future.result()
                        processed_count += 1
                        
                        if result:
                            if result['valid']:
                                validated_count += 1
                                # Check if likes or comments were updated
                                if result.get('likes', 0) > 0 or result.get('comments', 0) > 0:
                                    updated_count += 1
                                    status_msg = f'Processed {processed_count}/{total_count}: {result["link"][:50]}... (Likes: {result.get("likes", 0)}, Comments: {result.get("comments", 0)})'
                                else:
                                    status_msg = f'Processed {processed_count}/{total_count}: {result["link"][:50]}... ({result["reason"]})'
                            else:
                                failed_count += 1
                                status_msg = f'Processed {processed_count}/{total_count}: {result["link"][:50]}... ({result["reason"]})'
                            
                            # Yield progress with detailed counts
                            yield json.dumps({
                                'current': processed_count,
                                'total': total_count,
                                'validated': validated_count,
                                'failed': failed_count,
                                'updated': updated_count,
                                'status': status_msg
                            }) + '\n'
                    except Exception as e:
                        processed_count += 1
                        failed_count += 1
                        yield json.dumps({
                            'current': processed_count,
                            'total': total_count,
                            'validated': validated_count,
                            'failed': failed_count,
                            'updated': updated_count,
                            'status': f'Error processing: {str(e)}'
                        }) + '\n'
            
            # Final summary
            yield json.dumps({
                'current': total_count,
                'total': total_count,
                'status': 'Complete',
                'summary': f'Validated: {validated_count}, Failed: {failed_count}, Updated likes/comments: {updated_count}'
            }) + '\n'
            
        except Exception as e:
            yield json.dumps({'error': str(e)}) + '\n'

    return Response(stream_with_context(generate()), mimetype='application/json')


# ============================================
# Routes - Verification (Removed)
# ============================================

# ============================================
# Routes - Master Logs
# ============================================
@app.route('/logs')
@login_required
@permission_required('logs_list')
def logs_list():
    """View master logs"""
    try:
        table_filter = request.args.get('table', '')
        operation_filter = request.args.get('operation', '')
        limit = int(request.args.get('limit', 100))
        
        if table_filter:
            logs = MasterLogs.get_by_table(table_filter, limit=limit)
        elif operation_filter:
            logs = MasterLogs.get_by_operation(operation_filter, limit=limit)
        else:
            logs = MasterLogs.get_all(limit=limit)
        
        return render_template('logs_list.html', logs=logs, 
                             table_filter=table_filter, operation_filter=operation_filter)
    except Exception as e:
        flash(f'Error loading logs: {str(e)}', 'error')
        return render_template('logs_list.html', logs=[], table_filter='', operation_filter='')


@app.route('/api/logs')
def api_logs():
    """API endpoint for logs (JSON)"""
    try:
        table_filter = request.args.get('table', '')
        operation_filter = request.args.get('operation', '')
        limit = int(request.args.get('limit', 100))
        
        if table_filter:
            logs = MasterLogs.get_by_table(table_filter, limit=limit)
        elif operation_filter:
            logs = MasterLogs.get_by_operation(operation_filter, limit=limit)
        else:
            logs = MasterLogs.get_all(limit=limit)
        
        # Convert datetime objects to strings for JSON serialization
        for log in logs:
            if log.get('timestamp'):
                log['timestamp'] = log['timestamp'].isoformat() if hasattr(log['timestamp'], 'isoformat') else str(log['timestamp'])
        
        return jsonify(logs)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================
# Routes - Import
# ============================================
def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/import')
@login_required
@permission_required('import_page')
def import_page():
    """Import page - show options"""
    return render_template('import_index.html')


@app.route('/import/master')
@login_required
@permission_required('import_master_page')
def import_master_page():
    """Master workbook import page (12 sheets)"""
    return render_template('import_master.html')


@app.route('/import/advanced')
@login_required
@permission_required('import_advanced_page')
def import_advanced_page():
    """Advanced import page with column mapping"""
    return render_template('import_advanced.html')


@app.route('/import/simple')
def import_simple_page():
    """Simple import page (original)"""
    return render_template('import.html')


@app.route('/api/import/workshops', methods=['POST'])
def import_workshops():
    """Import master workshops XLSX file"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Only XLSX, XLS, and CSV files are allowed.'}), 400
        
        # Save file temporarily
        filename = secure_filename(file.filename)
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)
        
        # Parse workbook
        parse_result = parse_master_workbook(file_path)
        
        # Process and insert records
        total_inserted = 0
        total_updated = 0
        form_count = 0
        project_count = 0
        
        for sheet_result in parse_result['sheets']:
            records = sheet_result['records']
            if not records:
                continue
            
            try:
                if sheet_result['sheet_type'] == 'form':
                    result = FormResponse.bulk_upsert(records)
                    sheet_result['rows_inserted'] = result['inserted']
                    sheet_result['rows_updated'] = result['updated']
                    total_inserted += result['inserted']
                    total_updated += result['updated']
                    form_count += len(records)
                elif sheet_result['sheet_type'] == 'project':
                    result = ProjectSubmission.bulk_upsert(records)
                    sheet_result['rows_inserted'] = result['inserted']
                    sheet_result['rows_updated'] = result['updated']
                    total_inserted += result['inserted']
                    total_updated += result['updated']
                    project_count += len(records)
            except Exception as e:
                sheet_result['errors'].append(f"Database error: {str(e)}")
                parse_result['total_errors'].append(f"Sheet {sheet_result['sheet_index']}: {str(e)}")
        
        # Clean up temp file
        try:
            os.remove(file_path)
        except:
            pass
        
        # Prepare response
        response = {
            'success': True,
            'summary': {
                'workshops_processed': len(parse_result['workshops_processed']),
                'total_rows': parse_result['total_records'],
                'total_form_entries': form_count,
                'total_project_submissions': project_count,
                'rows_inserted': total_inserted,
                'rows_updated': total_updated,
                'total_errors': len(parse_result['total_errors'])
            },
            'sheets': parse_result['sheets'],
            'errors': parse_result['total_errors'][:100]  # Limit to first 100 errors
        }
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/api/import/user-pii', methods=['POST'])
def import_user_pii():
    """Import User PII XLSX file"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Only XLSX, XLS, and CSV files are allowed.'}), 400
        
        # Save file temporarily
        filename = secure_filename(file.filename)
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)
        
        # Parse workbook
        parse_result = parse_user_pii_workbook(file_path)
        
        # Insert/update records
        try:
            result = UserPII.bulk_upsert(parse_result['records'])
            parse_result['rows_inserted'] = result['inserted']
            parse_result['rows_updated'] = result['updated']
        except Exception as e:
            parse_result['errors'].append(f"Database error: {str(e)}")
        
        # Clean up temp file
        try:
            os.remove(file_path)
        except:
            pass
        
        # Prepare response
        response = {
            'success': True,
            'summary': {
                'rows_read': parse_result['rows_read'],
                'rows_inserted': parse_result['rows_inserted'],
                'rows_updated': parse_result['rows_updated'],
                'total_errors': len(parse_result['errors'])
            },
            'errors': parse_result['errors'][:100]  # Limit to first 100 errors
        }
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/api/import/master-preview', methods=['POST'])
def import_master_preview():
    """Preview master workbook with 12 sheets"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided', 'success': False}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected', 'success': False}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type', 'success': False}), 400
        
        # Save file temporarily
        filename = secure_filename(file.filename)
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)
        
        # Read workbook and extract all sheets
        try:
            from import_utils import read_xlsx_file, get_sheet_headers
            
            workbook = read_xlsx_file(file_path)
            sheets_info = []
            
            for idx, sheet_name in enumerate(workbook.sheetnames):
                sheet = workbook[sheet_name]
                headers = get_sheet_headers(sheet)
                
                # Get first 3 rows for preview
                preview = []
                for row in sheet.iter_rows(min_row=2, max_row=4, values_only=True):
                    if any(cell for cell in row):
                        preview.append(list(row))
                
                sheets_info.append({
                    'index': idx,
                    'name': sheet_name,
                    'columns': headers,
                    'preview': preview,
                    'total_rows': sheet.max_row - 1
                })
            
            workbook.close()
            
            result = {
                'success': True,
                'sheets': sheets_info,
                'total_sheets': len(sheets_info)
            }
        except Exception as e:
            result = {
                'success': False,
                'error': f'Error reading file: {str(e)}'
            }
        finally:
            # Clean up
            try:
                os.remove(file_path)
            except:
                pass
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/api/import/preview', methods=['POST'])
def import_preview():
    """Preview file columns and first few rows"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided', 'success': False}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected', 'success': False}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type', 'success': False}), 400
        
        # Save file temporarily
        filename = secure_filename(file.filename)
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)
        
        # Read file and extract columns
        try:
            from import_utils import read_xlsx_file, get_sheet_headers
            
            workbook = read_xlsx_file(file_path)
            sheet = workbook[workbook.sheetnames[0]]  # First sheet
            
            # Get headers
            headers = get_sheet_headers(sheet)
            
            # Get first 5 rows for preview
            preview = []
            for row in sheet.iter_rows(min_row=2, max_row=6, values_only=True):
                if any(cell for cell in row):  # Skip empty rows
                    preview.append(list(row))
            
            workbook.close()
            
            result = {
                'success': True,
                'columns': headers,
                'preview': preview,
                'total_rows': sheet.max_row - 1  # Exclude header
            }
        except Exception as e:
            result = {
                'success': False,
                'error': f'Error reading file: {str(e)}'
            }
        finally:
            # Clean up
            try:
                os.remove(file_path)
            except:
                pass
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/api/import/advanced', methods=['POST'])
def import_advanced():
    """Advanced import with column mapping"""
    try:
        print("=== Advanced Import Request Received ===")
        print(f"Request method: {request.method}")
        print(f"Request content type: {request.content_type}")
        print(f"Files in request: {list(request.files.keys())}")
        print(f"Form data keys: {list(request.form.keys())}")
        
        if 'file' not in request.files:
            print("ERROR: No file in request")
            return jsonify({'error': 'No file provided', 'success': False}), 400
        
        file = request.files['file']
        config_str = request.form.get('config', '{}')
        
        print(f"File name: {file.filename}")
        print(f"Config string: {config_str[:200]}...")
        
        if file.filename == '':
            print("ERROR: Empty filename")
            return jsonify({'error': 'No file selected', 'success': False}), 400
        
        if not allowed_file(file.filename):
            print(f"ERROR: Invalid file type: {file.filename}")
            return jsonify({'error': 'Invalid file type', 'success': False}), 400
        
        # Parse config
        try:
            config = json.loads(config_str)
            print(f"Parsed config: {config}")
        except Exception as e:
            print(f"ERROR parsing config: {e}")
            return jsonify({'error': f'Invalid configuration: {str(e)}', 'success': False}), 400
        
        table_name = config.get('table')
        import_mode = config.get('mode', 'create')
        mappings = config.get('mappings', {})
        match_fields = config.get('match_fields', [])
        
        print(f"Table: {table_name}, Mode: {import_mode}, Mappings: {len(mappings)}, Match fields: {match_fields}")
        
        if not table_name:
            print("ERROR: No table selected")
            return jsonify({'error': 'No table selected', 'success': False}), 400
        
        # Save file temporarily
        filename = secure_filename(file.filename)
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)
        
        # Process import
        try:
            from import_utils import read_xlsx_file, get_sheet_headers, validate_email, parse_datetime, normalize_string, coerce_boolean
            
            workbook = read_xlsx_file(file_path)
            sheet = workbook[workbook.sheetnames[0]]
            headers = get_sheet_headers(sheet)
            
            # Build column index map
            column_index_map = {}
            for db_field, file_column in mappings.items():
                if file_column and file_column in headers:
                    column_index_map[db_field] = headers.index(file_column)
            
            # Process rows
            records = []
            errors = []
            created = 0
            updated = 0
            skipped = 0
            
            for row_num, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                if not any(cell for cell in row):
                    continue
                
                try:
                    record = {}
                    for db_field, col_index in column_index_map.items():
                        if col_index < len(row):
                            value = row[col_index]
                            # Type conversion based on field name
                            if 'date' in db_field.lower() or 'time' in db_field.lower():
                                # Handle datetime parsing with better error handling
                                if value is not None:
                                    if not isinstance(value, str):
                                        value = str(value)
                                    if value.strip():
                                        parsed_dt = parse_datetime(value)
                                        if parsed_dt:
                                            record[db_field] = parsed_dt
                                        else:
                                            # If parsing fails, log and set to None
                                            print(f"Warning: Could not parse {db_field} '{value}' for row {row_num}")
                                            record[db_field] = None
                                    else:
                                        record[db_field] = None
                                else:
                                    record[db_field] = None
                            elif 'valid' in db_field.lower() or 'participated' in db_field.lower():
                                record[db_field] = coerce_boolean(value)
                            elif 'year' in db_field.lower():
                                try:
                                    record[db_field] = int(value) if value else None
                                except:
                                    record[db_field] = None
                            else:
                                record[db_field] = normalize_string(value)
                        else:
                            record[db_field] = None
                    
                    # Validate required fields
                    required_fields = {
                        'user_pii': ['email', 'name'],
                        'form_response': ['email', 'form_name', 'name'],
                        'aws_team_building': ['workshop_name', 'email', 'name'],
                        'project_submission': ['workshop_name', 'email', 'name'],
                        'verification': ['workshop_name', 'email', 'name']
                    }
                    
                    required = required_fields.get(table_name, [])
                    missing = [f for f in required if not record.get(f)]
                    if missing:
                        errors.append(f"Row {row_num}: Missing required fields: {', '.join(missing)}")
                        skipped += 1
                        continue
                    
                    # Validate email if present
                    if 'email' in record and record['email']:
                        if not validate_email(record['email']):
                            errors.append(f"Row {row_num}: Invalid email: {record['email']}")
                            skipped += 1
                            continue
                        record['email'] = record['email'].lower().strip()
                    
                    records.append(record)
                    
                except Exception as e:
                    errors.append(f"Row {row_num}: {str(e)}")
                    skipped += 1
                    continue
            
            workbook.close()
            
            # Import to database
            print(f"Processing {len(records)} records for table {table_name}")
            if records:
                try:
                    # Process records one by one to catch individual errors
                    created = 0
                    updated = 0
                    db_errors = []
                    
                    for idx, record in enumerate(records):
                        try:
                            if table_name == 'user_pii':
                                # Ensure registration_date_time has a default
                                if not record.get('registration_date_time'):
                                    record['registration_date_time'] = datetime.now()
                                single_result = bulk_upsert_advanced_user_pii([record], import_mode, match_fields)
                            elif table_name == 'form_response':
                                single_result = bulk_upsert_advanced_form_response([record], import_mode, match_fields)
                            elif table_name == 'aws_team_building':
                                single_result = bulk_upsert_advanced_aws_team_building([record], import_mode, match_fields)
                            elif table_name == 'project_submission':
                                single_result = bulk_upsert_advanced_project_submission([record], import_mode, match_fields)
                            elif table_name == 'verification':
                                single_result = bulk_upsert_advanced_verification([record], import_mode, match_fields)
                            else:
                                single_result = {'inserted': 0, 'updated': 0}
                            
                            created += single_result.get('inserted', 0)
                            updated += single_result.get('updated', 0)
                        except Exception as record_error:
                            error_msg = str(record_error)
                            # Extract row number from record if available
                            row_info = f"Row {record.get('row_number', idx + 2)}" if 'row_number' in record else f"Record {idx + 1}"
                            db_errors.append(f"{row_info}: {error_msg}")
                            print(f"Error processing record {idx + 1}: {error_msg}")
                            skipped += 1
                    
                    print(f"Import complete: {created} created, {updated} updated, {len(db_errors)} database errors")
                    errors.extend(db_errors)
                except Exception as db_error:
                    print(f"Database error: {db_error}")
                    import traceback
                    traceback.print_exc()
                    errors.append(f"Database error: {str(db_error)}")
                    # Don't raise, continue to return partial results
            
            response = {
                'success': True,
                'summary': {
                    'total_rows': len(records) + skipped,
                    'created': created,
                    'updated': updated,
                    'skipped': skipped,
                    'errors': len(errors)
                },
                'errors': errors[:100]  # Limit errors
            }
            print(f"Response: {response}")
            
        except Exception as e:
            print(f"Exception in import processing: {e}")
            import traceback
            traceback.print_exc()
            response = {
                'success': False,
                'error': str(e)
            }
        finally:
            # Clean up
            try:
                os.remove(file_path)
            except:
                pass
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/api/import/master', methods=['POST'])
def import_master():
    """Import master workbook with workshop selection"""
    try:
        print("=== Master Workbook Import Request Received ===")
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided', 'success': False}), 400
        
        file = request.files['file']
        config_str = request.form.get('config', '{}')
        
        if file.filename == '':
            return jsonify({'error': 'No file selected', 'success': False}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type', 'success': False}), 400
        
        # Parse config
        try:
            config = json.loads(config_str)
            print(f"Master import config: {config}")
        except Exception as e:
            print(f"ERROR parsing config: {e}")
            return jsonify({'error': f'Invalid configuration: {str(e)}', 'success': False}), 400
        
        workshop_num = config.get('workshop_num')
        workshop_name = config.get('workshop_name', f'Workshop {workshop_num}')
        import_mode = config.get('mode', 'create')
        import_type = config.get('import_type', 'both')  # 'form', 'project', or 'both'
        form_mappings = config.get('form_mappings', {})
        form_match_fields = config.get('form_match_fields', [])
        project_mappings = config.get('project_mappings', {})
        project_match_fields = config.get('project_match_fields', [])
        form_sheet_index = config.get('form_sheet_index')
        project_sheet_index = config.get('project_sheet_index')
        
        if workshop_num is None:
            return jsonify({'error': 'No workshop selected', 'success': False}), 400
        
        # Validate import_type
        if import_type not in ['form', 'project', 'both']:
            return jsonify({'error': 'Invalid import_type. Must be "form", "project", or "both"', 'success': False}), 400
        
        # Save file temporarily
        filename = secure_filename(file.filename)
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)
        
        # Process import
        try:
            from import_utils import read_xlsx_file, get_sheet_headers, validate_email, parse_datetime, normalize_string, coerce_boolean
            
            workbook = read_xlsx_file(file_path)
            
            # Process Form Response sheet (only if import_type is 'form' or 'both')
            form_records = []
            form_errors = []
            form_created = 0
            form_updated = 0
            form_skipped = 0
            
            if (import_type == 'form' or import_type == 'both') and form_sheet_index is not None and form_sheet_index < len(workbook.sheetnames):
                form_sheet = workbook[workbook.sheetnames[form_sheet_index]]
                form_headers = get_sheet_headers(form_sheet)
                
                # Build column index map
                form_column_index_map = {}
                for db_field, file_column in form_mappings.items():
                    if file_column and file_column in form_headers:
                        form_column_index_map[db_field] = form_headers.index(file_column)
                
                # Process form rows
                for row_num, row in enumerate(form_sheet.iter_rows(min_row=2, values_only=True), start=2):
                    if not any(cell for cell in row):
                        continue
                    
                    try:
                        record = {
                            'email': None,
                            'name': None,
                            'form_name': workshop_name,  # Auto-fill
                            'time_slot': None
                        }
                        
                        for db_field, col_index in form_column_index_map.items():
                            if col_index < len(row):
                                value = row[col_index]
                                if db_field == 'time_slot':
                                    # Convert to string if needed, handle None/empty
                                    if value is not None:
                                        if not isinstance(value, str):
                                            value = str(value)
                                        if value.strip():
                                            # Store original value for full range display
                                            record['time_slot_original'] = value.strip()
                                            parsed_time = parse_datetime(value)
                                            if parsed_time:
                                                record[db_field] = parsed_time
                                            else:
                                                # If parsing fails, store as string for debugging
                                                print(f"Warning: Could not parse time_slot '{value}' for row {row_num}")
                                                record[db_field] = None
                                        else:
                                            record[db_field] = None
                                    else:
                                        record[db_field] = None
                                else:
                                    record[db_field] = normalize_string(value)
                        
                        # Validate required
                        if not record.get('email') or not record.get('name'):
                            form_errors.append(f"Row {row_num}: Missing email or name")
                            form_skipped += 1
                            continue
                        
                        if not validate_email(record['email']):
                            form_errors.append(f"Row {row_num}: Invalid email: {record['email']}")
                            form_skipped += 1
                            continue
                        
                        record['email'] = record['email'].lower().strip()
                        form_records.append(record)
                        
                    except Exception as e:
                        form_errors.append(f"Row {row_num}: {str(e)}")
                        form_skipped += 1
                        continue
                
                # Import form records
                if form_records:
                    for record in form_records:
                        try:
                            single_result = bulk_upsert_advanced_form_response([record], import_mode, form_match_fields)
                            form_created += single_result.get('inserted', 0)
                            form_updated += single_result.get('updated', 0)
                        except Exception as e:
                            form_errors.append(f"Form record error: {str(e)}")
                            form_skipped += 1
            
            # Process Project Submission sheet (only if import_type is 'project' or 'both')
            project_records = []
            project_errors = []
            project_created = 0
            project_updated = 0
            project_skipped = 0
            
            if (import_type == 'project' or import_type == 'both') and project_sheet_index is not None and project_sheet_index < len(workbook.sheetnames):
                project_sheet = workbook[workbook.sheetnames[project_sheet_index]]
                project_headers = get_sheet_headers(project_sheet)
                
                # Build column index map
                project_column_index_map = {}
                for db_field, file_column in project_mappings.items():
                    if file_column and file_column in project_headers:
                        project_column_index_map[db_field] = project_headers.index(file_column)
                
                # Process project rows
                for row_num, row in enumerate(project_sheet.iter_rows(min_row=2, values_only=True), start=2):
                    if not any(cell for cell in row):
                        continue
                    
                    try:
                        record = {
                            'workshop_name': workshop_name,  # Auto-fill
                            'email': None,
                            'name': None,
                            'project_link': None,
                            'valid': False,
                            'team_id': None
                        }
                        
                        for db_field, col_index in project_column_index_map.items():
                            if col_index < len(row):
                                value = row[col_index]
                                if db_field == 'valid':
                                    record[db_field] = coerce_boolean(value)
                                else:
                                    record[db_field] = normalize_string(value)
                        
                        # Validate required
                        if not record.get('email') or not record.get('name'):
                            project_errors.append(f"Row {row_num}: Missing email or name")
                            project_skipped += 1
                            continue
                        
                        if not validate_email(record['email']):
                            project_errors.append(f"Row {row_num}: Invalid email: {record['email']}")
                            project_skipped += 1
                            continue
                        
                        record['email'] = record['email'].lower().strip()
                        project_records.append(record)
                        
                    except Exception as e:
                        project_errors.append(f"Row {row_num}: {str(e)}")
                        project_skipped += 1
                        continue
                
                # Import project records
                if project_records:
                    for record in project_records:
                        try:
                            single_result = bulk_upsert_advanced_project_submission([record], import_mode, project_match_fields)
                            project_created += single_result.get('inserted', 0)
                            project_updated += single_result.get('updated', 0)
                        except Exception as e:
                            project_errors.append(f"Project record error: {str(e)}")
                            project_skipped += 1
            
            workbook.close()
            
            all_errors = form_errors + project_errors
            
            response = {
                'success': True,
                'summary': {
                    'workshop_name': workshop_name,
                    'form_rows': len(form_records) + form_skipped,
                    'form_created': form_created,
                    'form_updated': form_updated,
                    'form_skipped': form_skipped,
                    'project_rows': len(project_records) + project_skipped,
                    'project_created': project_created,
                    'project_updated': project_updated,
                    'project_skipped': project_skipped,
                    'errors': len(all_errors)
                },
                'errors': all_errors[:100]
            }
            print(f"Master import response: {response}")
            
        except Exception as e:
            print(f"Exception in master import: {e}")
            import traceback
            traceback.print_exc()
            response = {
                'success': False,
                'error': str(e)
            }
        finally:
            # Clean up
            try:
                os.remove(file_path)
            except:
                pass
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


# ============================================
# Error Handlers
# ============================================
@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500


# ============================================
# Routes - Google Sheets Export
# ============================================
@app.route('/api/export-to-sheet', methods=['POST'])
def export_to_sheet():
    """
    Export workshop, time slot, and occupation data to Google Sheet
    
    Column structure:
    - Column A: Workshop Number
    - Column B: Time Slot Number (1, 2, 3 based on chronological order per workshop)
    - Column C: Time Slot (date/time string)
    - Column D: Occupation
    - Column E: Count
    
    Expected request body:
    {
        "sheet_id": "your-google-sheet-id",
        "clear_first": true/false (optional, default: true)
    }
    
    Returns:
    {
        "success": true/false,
        "message": "Export completed successfully",
        "rows_exported": 100
    }
    """
    try:
        data = request.get_json() or {}
        sheet_id = data.get('sheet_id') or os.getenv('GOOGLE_SHEET_ID')
        clear_first = data.get('clear_first', True)
        
        if not sheet_id:
            return jsonify({
                'success': False,
                'error': 'Google Sheet ID not provided. Please provide sheet_id in request body or set GOOGLE_SHEET_ID environment variable.'
            }), 400
        
        # Fetch data from PostgreSQL
        # Query: Workshop Number, Time Slot Number, Time Slot, Occupation, Count
        # Extract workshop number from form_name (e.g., "Workshop 1" -> 1)
        # Calculate time slot number based on chronological order per workshop
        query = """
            WITH workshop_data AS (
                SELECT 
                    CASE 
                        WHEN fr.form_name ~ '^Workshop ([0-9]+)' THEN 
                            CAST(SUBSTRING(fr.form_name FROM '^Workshop ([0-9]+)') AS INTEGER)
                        ELSE NULL
                    END as workshop_number,
                    COALESCE(
                        NULLIF(fr.time_slot_range, ''),
                        CASE 
                            WHEN fr.time_slot IS NOT NULL 
                            THEN TO_CHAR(fr.time_slot::TIMESTAMP, 'YYYY-MM-DD HH24:MI')
                            ELSE 'No Time Slot'
                        END
                    ) as time_slot,
                    -- Extract date for ordering: use time_slot if available, otherwise parse time_slot_range
                    CASE 
                        WHEN fr.time_slot IS NOT NULL THEN fr.time_slot::DATE
                        WHEN fr.time_slot_range IS NOT NULL AND fr.time_slot_range != '' THEN
                            -- Try to parse date from format like "25 Nov, 4:00 - 7:00 PM"
                            -- Extract the date part (e.g., "25 Nov") and parse it
                            CASE 
                                WHEN fr.time_slot_range ~ '^([0-9]+) [A-Za-z]{3}' THEN
                                    TO_DATE(SUBSTRING(fr.time_slot_range FROM '^([0-9]+ [A-Za-z]{3})'), 'DD Mon')
                                ELSE NULL
                            END
                        ELSE NULL
                    END as slot_date,
                    COALESCE(NULLIF(u.occupation, ''), 'Unknown') as occupation
                FROM form_response fr
                LEFT JOIN user_pii u ON fr.email = u.email
                WHERE fr.form_name LIKE 'Workshop %'
            ),
            ranked_data AS (
                SELECT 
                    workshop_number,
                    time_slot,
                    slot_date,
                    occupation,
                    -- Assign time slot number based on chronological order per workshop
                    DENSE_RANK() OVER (
                        PARTITION BY workshop_number 
                        ORDER BY slot_date NULLS LAST
                    ) as time_slot_number
                FROM workshop_data
                WHERE workshop_number IS NOT NULL
            )
            SELECT 
                workshop_number,
                time_slot_number,
                time_slot,
                occupation,
                COUNT(*) as count
            FROM ranked_data
            GROUP BY workshop_number, time_slot_number, time_slot, occupation
            ORDER BY workshop_number, time_slot_number, occupation
        """
        
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        db_manager.return_connection(conn)
        
        if not rows:
            return jsonify({
                'success': False,
                'error': 'No data found to export'
            }), 404
        
        # Prepare data for Google Sheets
        # Header row
        sheet_data = [
            ['Workshop Number', 'Time Slot Number', 'Time Slot', 'Occupation', 'Count']
        ]
        
        # Data rows
        for row in rows:
            workshop_num, time_slot_number, time_slot, occupation, count = row
            sheet_data.append([
                workshop_num if workshop_num else '',
                time_slot_number if time_slot_number else '',
                time_slot if time_slot else 'No Time Slot',
                occupation if occupation else 'Unknown',
                count if count else 0
            ])
        
        # Export to Google Sheet
        exporter = GoogleSheetsExporter(sheet_id=sheet_id)
        result = exporter.write_data(
            data=sheet_data,
            range_name='Sheet1!A1',
            clear_first=clear_first
        )
        
        rows_exported = len(sheet_data) - 1  # Exclude header
        
        return jsonify({
            'success': True,
            'message': f'Successfully exported {rows_exported} rows to Google Sheet',
            'rows_exported': rows_exported,
            'sheet_id': sheet_id
        })
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'error': f'Credentials file not found: {str(e)}'
        }), 500
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error exporting to Google Sheet: {error_trace}")
        return jsonify({
            'success': False,
            'error': f'Failed to export to Google Sheet: {str(e)}'
        }), 500


# ============================================
# Routes - RBAC Admin (User & Permission Management)
# ============================================

@app.route('/admin/users')
@login_required
@admin_required
def admin_users_list():
    """List all RBAC users"""
    try:
        users = RBACUser.list_all()
        # Format dates for template
        formatted_users = []
        for user in users:
            user_dict = dict(user) if isinstance(user, dict) else {
                'user_id': user[0],
                'username': user[1],
                'email': user[2],
                'full_name': user[3],
                'is_admin': user[4],
                'is_active': user[5],
                'created_at': user[6] if len(user) > 6 else None,
                'last_login': user[7] if len(user) > 7 else None
            }
            # Format last_login if it exists
            if user_dict.get('last_login'):
                last_login = user_dict['last_login']
                if hasattr(last_login, 'strftime'):
                    user_dict['last_login'] = last_login.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    user_dict['last_login'] = str(last_login)
            # Format created_at if it exists
            if user_dict.get('created_at'):
                created_at = user_dict['created_at']
                if hasattr(created_at, 'strftime'):
                    user_dict['created_at'] = created_at.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    user_dict['created_at'] = str(created_at)
            formatted_users.append(user_dict)
        return render_template('admin_users_list.html', users=formatted_users)
    except Exception as e:
        flash(f'Error loading users: {str(e)}', 'error')
        return render_template('admin_users_list.html', users=[])

@app.route('/admin/users/create', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_user_create():
    """Create new RBAC user"""
    if request.method == 'POST':
        try:
            username = request.form.get('username', '').strip()
            email = request.form.get('email', '').strip()
            password = request.form.get('password', '')
            full_name = request.form.get('full_name', '').strip()
            is_admin = request.form.get('is_admin') == 'on'
            
            if not username or not email or not password:
                flash('Username, email, and password are required', 'error')
                return render_template('admin_user_form.html', user=None)
            
            # Check if username or email already exists
            if RBACUser.get_by_username(username):
                flash('Username already exists', 'error')
                return render_template('admin_user_form.html', user=None)
            if RBACUser.get_by_email(email):
                flash('Email already exists', 'error')
                return render_template('admin_user_form.html', user=None)
            
            RBACUser.create(username, email, password, full_name, is_admin)
            flash('User created successfully!', 'success')
            return redirect(url_for('admin_users_list'))
        except Exception as e:
            flash(f'Error creating user: {str(e)}', 'error')
    
    return render_template('admin_user_form.html', user=None)

@app.route('/admin/users/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_user_edit(user_id):
    """Edit RBAC user"""
    user = RBACUser.get_by_id(user_id)
    if not user:
        flash('User not found', 'error')
        return redirect(url_for('admin_users_list'))
    
    if request.method == 'POST':
        try:
            username = request.form.get('username', '').strip()
            email = request.form.get('email', '').strip()
            full_name = request.form.get('full_name', '').strip()
            is_admin = request.form.get('is_admin') == 'on'
            is_active = request.form.get('is_active') == 'on'
            password = request.form.get('password', '').strip()
            
            # Check if username or email is taken by another user
            existing_user = RBACUser.get_by_username(username)
            if existing_user and existing_user['user_id'] != user_id:
                flash('Username already exists', 'error')
                return render_template('admin_user_form.html', user=user)
            
            existing_user = RBACUser.get_by_email(email)
            if existing_user and existing_user['user_id'] != user_id:
                flash('Email already exists', 'error')
                return render_template('admin_user_form.html', user=user)
            
            update_data = {
                'username': username,
                'email': email,
                'full_name': full_name,
                'is_admin': is_admin,
                'is_active': is_active
            }
            if password:
                update_data['password'] = password
            
            RBACUser.update(user_id, **update_data)
            flash('User updated successfully!', 'success')
            return redirect(url_for('admin_users_list'))
        except Exception as e:
            flash(f'Error updating user: {str(e)}', 'error')
    
    return render_template('admin_user_form.html', user=user)

@app.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def admin_user_delete(user_id):
    """Delete RBAC user"""
    if user_id == session.get('user_id'):
        flash('You cannot delete your own account', 'error')
        return redirect(url_for('admin_users_list'))
    
    try:
        RBACUser.delete(user_id)
        flash('User deleted successfully!', 'success')
    except Exception as e:
        flash(f'Error deleting user: {str(e)}', 'error')
    
    return redirect(url_for('admin_users_list'))

@app.route('/admin/users/<int:user_id>/permissions', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_user_permissions(user_id):
    """Manage permissions for a user"""
    user = RBACUser.get_by_id(user_id)
    if not user:
        flash('User not found', 'error')
        return redirect(url_for('admin_users_list'))
    
    if request.method == 'POST':
        try:
            permission_ids = [int(pid) for pid in request.form.getlist('permissions')]
            RBACUserPermission.set_user_permissions(user_id, permission_ids, session.get('user_id'))
            flash('Permissions updated successfully!', 'success')
            return redirect(url_for('admin_user_permissions', user_id=user_id))
        except Exception as e:
            flash(f'Error updating permissions: {str(e)}', 'error')
    
    # Get all permissions
    all_permissions = RBACPermission.get_all()
    
    # Get user's current permissions
    user_permissions = RBACUserPermission.get_user_permissions(user_id)
    user_permission_ids = {p['permission_id'] if isinstance(p, dict) else p[0] for p in user_permissions}
    
    # Group permissions by category
    permissions_by_category = {}
    for perm in all_permissions:
        category = perm.get('category', 'Other') if isinstance(perm, dict) else perm[3] if len(perm) > 3 else 'Other'
        if category not in permissions_by_category:
            permissions_by_category[category] = []
        permissions_by_category[category].append(perm)
    
    return render_template('admin_user_permissions.html', 
                         user=user, 
                         permissions_by_category=permissions_by_category,
                         user_permission_ids=user_permission_ids)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=4000)

