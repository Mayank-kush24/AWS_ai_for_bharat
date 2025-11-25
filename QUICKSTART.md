# Quick Start Guide

## 1. Install Dependencies

```bash
pip install -r requirements.txt
```

## 2. Setup Database

Create a `.env` file in the project root:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=aws_ai_bharat
DB_USER=postgres
DB_PASSWORD=your_password_here
```

## 3. Create Database

```sql
CREATE DATABASE aws_ai_bharat;
```

## 4. Initialize Schema

```bash
psql -U postgres -d aws_ai_bharat -f schema.sql
```

## 5. Run Web Application

```bash
python app_web.py
```

## 6. Access the Application

Open your browser and go to: **http://localhost:4000**

## Features Available

- ✅ Dashboard with statistics
- ✅ User management (Create, Read, Update)
- ✅ Form Response tracking
- ✅ AWS Team Building records
- ✅ Project Submissions
- ✅ Verifications
- ✅ Automatic Activity Logs (Master Logs)

All database operations are automatically logged to the Master Logs table!

