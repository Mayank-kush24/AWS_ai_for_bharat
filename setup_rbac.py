"""
RBAC Setup Script
Creates the default admin user with password 'admin123'
Run this script once after running the migration_rbac_schema.sql

Usage:
    python setup_rbac.py [password]
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from database import db_manager, RBACUser, RBACPermission, RBACUserPermission
import bcrypt

def setup_rbac(password=None):
    """Setup RBAC system with default admin user"""
    try:
        # Create connection pool
        db_manager.create_pool()
        
        # Check if admin user exists
        admin = RBACUser.get_by_username('admin')
        
        if admin:
            print("Admin user already exists. Skipping creation.")
            try:
                response = input("Do you want to reset the admin password? (y/n): ")
                if response.lower() == 'y':
                    new_password = input("Enter new password for admin: ")
                    if new_password:
                        admin_id = admin['user_id'] if isinstance(admin, dict) else admin[0]
                        RBACUser.update(admin_id, password=new_password)
                        print("Admin password updated successfully!")
            except (EOFError, KeyboardInterrupt):
                print("Skipping password reset.")
        else:
            # Create admin user
            print("Creating default admin user...")
            if not password:
                try:
                    password = input("Enter password for admin user (default: admin123): ").strip()
                except (EOFError, KeyboardInterrupt):
                    password = 'admin123'
                    print("Using default password: admin123")
            
            if not password:
                password = 'admin123'
                print("Using default password: admin123")
            
            admin_user = RBACUser.create(
                username='admin',
                email='admin@awsai4bharat.com',
                password=password,
                full_name='System Administrator',
                is_admin=True
            )
            
            print(f"Admin user created successfully! Username: admin")
            print(f"IMPORTANT: Please change the password after first login!")
        
        # Ensure all permissions are granted to admin
        admin = RBACUser.get_by_username('admin')
        if admin:
            admin_id = admin['user_id'] if isinstance(admin, dict) else admin[0]
            all_permissions = RBACPermission.get_all()
            permission_ids = [p['permission_id'] if isinstance(p, dict) else p[0] for p in all_permissions]
            RBACUserPermission.set_user_permissions(admin_id, permission_ids, admin_id)
            print(f"All permissions granted to admin user.")
        
        print("\nRBAC setup completed successfully!")
        print("\nYou can now login with:")
        print("  Username: admin")
        print(f"  Password: {password if 'password' in locals() else '(the password you set)'}")
        
    except Exception as e:
        print(f"Error setting up RBAC: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    admin_password = sys.argv[1] if len(sys.argv) > 1 else None
    setup_rbac(password=admin_password)

