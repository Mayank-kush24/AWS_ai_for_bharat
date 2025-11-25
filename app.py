"""
Main application for AWS AI for Bharat Tracking System
Provides CLI interface for database operations
"""
import sys
from datetime import datetime
from database import (
    db_manager, UserPII, FormResponse, AWSTeamBuilding,
    ProjectSubmission, Verification, MasterLogs
)


def print_menu():
    """Print main menu"""
    print("\n" + "="*60)
    print("AWS AI for Bharat Tracking System")
    print("="*60)
    print("1. User PII Operations")
    print("2. Form Response Operations")
    print("3. AWS Team Building Operations")
    print("4. Project Submission Operations")
    print("5. Verification Operations")
    print("6. Master Logs (View Activity)")
    print("7. Initialize Database Schema")
    print("0. Exit")
    print("="*60)


def user_pii_menu():
    """User PII operations menu"""
    while True:
        print("\n--- User PII Operations ---")
        print("1. Create User")
        print("2. Get User by Email")
        print("3. Update User")
        print("4. List All Users")
        print("0. Back to Main Menu")
        
        choice = input("\nEnter choice: ").strip()
        
        if choice == '1':
            email = input("Email: ").strip()
            name = input("Name: ").strip()
            phone = input("Phone Number (optional): ").strip() or None
            gender = input("Gender (optional): ").strip() or None
            country = input("Country (optional): ").strip() or None
            state = input("State (optional): ").strip() or None
            city = input("City (optional): ").strip() or None
            
            try:
                UserPII.create(email, name, phone_number=phone, gender=gender,
                              country=country, state=state, city=city)
                print(f"✓ User {email} created successfully")
            except Exception as e:
                print(f"✗ Error: {e}")
        
        elif choice == '2':
            email = input("Email: ").strip()
            user = UserPII.get(email)
            if user:
                print("\nUser Details:")
                for key, value in user.items():
                    print(f"  {key}: {value}")
            else:
                print("User not found")
        
        elif choice == '3':
            email = input("Email: ").strip()
            print("Enter new values (press Enter to skip):")
            phone = input("Phone Number: ").strip() or None
            gender = input("Gender: ").strip() or None
            country = input("Country: ").strip() or None
            
            updates = {}
            if phone: updates['phone_number'] = phone
            if gender: updates['gender'] = gender
            if country: updates['country'] = country
            
            try:
                UserPII.update(email, **updates)
                print(f"✓ User {email} updated successfully")
            except Exception as e:
                print(f"✗ Error: {e}")
        
        elif choice == '4':
            users = UserPII.list_all()
            print(f"\nTotal Users: {len(users)}")
            for user in users[:10]:  # Show first 10
                print(f"  - {user['email']}: {user['name']}")
        
        elif choice == '0':
            break


def master_logs_menu():
    """Master Logs operations menu"""
    while True:
        print("\n--- Master Logs (Activity Tracking) ---")
        print("1. View All Recent Logs")
        print("2. View Logs by Table")
        print("3. View Logs by Operation Type")
        print("4. View Logs for Specific Record")
        print("0. Back to Main Menu")
        
        choice = input("\nEnter choice: ").strip()
        
        if choice == '1':
            limit = input("Number of logs to show (default 50): ").strip() or "50"
            logs = MasterLogs.get_all(limit=int(limit))
            print(f"\nShowing {len(logs)} recent logs:")
            for log in logs:
                print(f"\n[{log['timestamp']}] {log['operation_type']} on {log['table_name']}")
                print(f"  Record: {log['record_identifier']}")
        
        elif choice == '2':
            table = input("Table name: ").strip()
            logs = MasterLogs.get_by_table(table)
            print(f"\nShowing {len(logs)} logs for {table}:")
            for log in logs:
                print(f"\n[{log['timestamp']}] {log['operation_type']}")
                print(f"  Record: {log['record_identifier']}")
        
        elif choice == '3':
            op_type = input("Operation type (INSERT/UPDATE/DELETE): ").strip().upper()
            logs = MasterLogs.get_by_operation(op_type)
            print(f"\nShowing {len(logs)} {op_type} operations:")
            for log in logs:
                print(f"\n[{log['timestamp']}] {log['table_name']}: {log['record_identifier']}")
        
        elif choice == '4':
            table = input("Table name: ").strip()
            record_id = input("Record identifier: ").strip()
            logs = MasterLogs.get_by_record(table, record_id)
            print(f"\nShowing {len(logs)} logs for record:")
            for log in logs:
                print(f"\n[{log['timestamp']}] {log['operation_type']}")
                if log['old_values']:
                    print(f"  Old: {log['old_values']}")
                if log['new_values']:
                    print(f"  New: {log['new_values']}")
        
        elif choice == '0':
            break


def main():
    """Main application loop"""
    print("Initializing database connection...")
    try:
        db_manager.create_pool()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Please check your .env file and database configuration")
        sys.exit(1)
    
    try:
        while True:
            print_menu()
            choice = input("\nEnter choice: ").strip()
            
            if choice == '1':
                user_pii_menu()
            elif choice == '6':
                master_logs_menu()
            elif choice == '7':
                print("Initializing database schema...")
                try:
                    db_manager.initialize_database()
                    print("✓ Database schema initialized successfully")
                except Exception as e:
                    print(f"✗ Error: {e}")
            elif choice == '0':
                print("Goodbye!")
                break
            else:
                print("Invalid choice. Please try again.")
    
    except KeyboardInterrupt:
        print("\n\nExiting...")
    finally:
        db_manager.close_pool()


if __name__ == "__main__":
    main()

