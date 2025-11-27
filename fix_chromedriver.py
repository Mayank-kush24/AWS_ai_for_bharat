"""
Script to fix ChromeDriver issues and test Selenium setup
"""
import os
import shutil
from pathlib import Path

def clear_webdriver_cache():
    """Clear webdriver-manager cache to force re-download"""
    cache_dir = Path.home() / ".wdm"
    if cache_dir.exists():
        print(f"Clearing webdriver-manager cache at: {cache_dir}")
        try:
            shutil.rmtree(cache_dir)
            print("Cache cleared successfully")
        except Exception as e:
            print(f"Error clearing cache: {e}")
    else:
        print("No cache directory found")

def test_chromedriver():
    """Test ChromeDriver installation"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        print("Testing ChromeDriver setup...")
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        # Try to get ChromeDriver path
        try:
            driver_path = ChromeDriverManager().install()
            print(f"[OK] ChromeDriverManager returned: {driver_path}")
            
            # Fix: Find actual chromedriver.exe
            driver_dir = os.path.dirname(driver_path)
            print(f"[INFO] Looking in directory: {driver_dir}")
            
            actual_driver = None
            if os.path.isdir(driver_dir):
                print(f"[INFO] Files in directory:")
                for file in os.listdir(driver_dir):
                    file_path = os.path.join(driver_dir, file)
                    size = os.path.getsize(file_path) if os.path.isfile(file_path) else 0
                    print(f"  - {file} ({size:,} bytes)")
                    if file == 'chromedriver.exe':
                        actual_driver = file_path
                        print(f"[OK] Found chromedriver.exe!")
            
            if actual_driver:
                driver_path = actual_driver
                print(f"[OK] Using ChromeDriver at: {driver_path}")
            else:
                print(f"[WARNING] chromedriver.exe not found, trying original path")
            
            # Verify file
            if os.path.exists(driver_path):
                size = os.path.getsize(driver_path)
                print(f"[OK] File exists, size: {size:,} bytes")
                
                # Try to initialize
                service = Service(driver_path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
                print("[OK] ChromeDriver initialized successfully!")
                driver.quit()
                return True
            else:
                print("[ERROR] ChromeDriver file not found")
                return False
        except Exception as e:
            print(f"[ERROR] Error: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    except ImportError as e:
        print(f"✗ Selenium not installed: {e}")
        print("Install with: pip install selenium webdriver-manager")
        return False

if __name__ == '__main__':
    print("="*60)
    print("ChromeDriver Fix Script")
    print("="*60)
    
    print("\n1. Clearing webdriver-manager cache...")
    clear_webdriver_cache()
    
    print("\n2. Testing ChromeDriver...")
    success = test_chromedriver()
    
    if success:
        print("\n[SUCCESS] ChromeDriver is working correctly!")
    else:
        print("\n[FAILED] ChromeDriver setup failed.")
        print("\nManual fix options:")
        print("1. Download ChromeDriver manually from: https://chromedriver.chromium.org/")
        print("2. Extract and place chromedriver.exe in your project folder")
        print("3. Or add it to your system PATH")
        print("4. Make sure it matches your Chrome browser version")

