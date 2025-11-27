"""
Test script using Selenium to scrape JavaScript-rendered blog pages
Focuses on:
- Like button with aria-label='Like this article'
- Comment button with aria-label='Comment on this article'

Requires: pip install selenium webdriver-manager
"""
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    from bs4 import BeautifulSoup
    import re
    import time
    import os
    import zipfile
    SELENIUM_AVAILABLE = True
except ImportError as e:
    SELENIUM_AVAILABLE = False
    print(f"Selenium not installed. Install with: pip install selenium webdriver-manager")
    print(f"Error: {e}")

def scrape_with_selenium(blog_url):
    """Scrape using Selenium to render JavaScript"""
    if not SELENIUM_AVAILABLE:
        return 0, 0, "Selenium not available"
    
    likes = 0
    comments = 0
    error = None
    
    try:
        # Setup Chrome options
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Run in background
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Initialize driver with webdriver-manager
        print("Setting up Chrome driver...")
        try:
            driver_path = ChromeDriverManager().install()
            print(f"ChromeDriverManager returned: {driver_path}")
            
            # Fix: webdriver-manager sometimes returns wrong file, find the actual chromedriver.exe
            driver_dir = os.path.dirname(driver_path)
            actual_driver = None
            
            # Check if ChromeDriver is in a zip file and extract it
            zip_files = [f for f in os.listdir(driver_dir) if f.endswith('.zip')]
            if zip_files:
                zip_path = os.path.join(driver_dir, zip_files[0])
                print(f"Found zip file: {zip_path}, extracting...")
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(driver_dir)
                    print("Extracted ChromeDriver from zip")
                except Exception as e:
                    print(f"Failed to extract zip: {e}")
            
            # Recursively search for chromedriver.exe
            def find_chromedriver_exe(search_dir):
                """Recursively search for chromedriver.exe"""
                if not os.path.isdir(search_dir):
                    return None
                for root, dirs, files in os.walk(search_dir):
                    for file in files:
                        if file == 'chromedriver.exe':
                            full_path = os.path.join(root, file)
                            # Verify it's actually an executable (should be > 1MB)
                            try:
                                if os.path.getsize(full_path) > 1000000:
                                    return full_path
                            except:
                                pass
                return None
            
            # Search in driver directory
            actual_driver = find_chromedriver_exe(driver_dir)
            if not actual_driver:
                # Try parent directory
                parent_dir = os.path.dirname(driver_dir)
                actual_driver = find_chromedriver_exe(parent_dir)
            
            if actual_driver and os.path.exists(actual_driver):
                driver_path = actual_driver
                print(f"Found actual ChromeDriver at: {driver_path}")
            else:
                print(f"Using ChromeDriverManager path: {driver_path}")
            
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            print("Chrome driver initialized successfully")
        except Exception as e:
            print(f"Failed to initialize ChromeDriver with webdriver-manager: {e}")
            # Try without webdriver-manager (if ChromeDriver is in PATH)
            try:
                print("Trying ChromeDriver from PATH...")
                driver = webdriver.Chrome(options=chrome_options)
                print("Chrome driver initialized from PATH")
            except Exception as e2:
                raise Exception(f"ChromeDriver initialization failed. PATH error: {e2}. Webdriver-manager error: {e}")
        
        print(f"Loading page: {blog_url}")
        driver.get(blog_url)
        
        # Wait for page to load (wait for body or specific elements)
        print("Waiting for page to load...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Wait a bit more for dynamic content
        time.sleep(3)
        
        # Get page source after JavaScript execution
        page_source = driver.page_source
        
        # Save rendered HTML
        with open('blog_page_rendered.html', 'w', encoding='utf-8') as f:
            f.write(page_source)
        print("Saved rendered HTML to blog_page_rendered.html")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Method 1: Find Like button with exact aria-label "Like this article"
        print("\n=== Searching for 'Like this article' button ===")
        try:
            like_buttons = driver.find_elements(By.XPATH, "//button[@aria-label='Like this article']")
            if not like_buttons:
                # Fallback to contains
                like_buttons = driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Like this article')]")
            
            print(f"Found {len(like_buttons)} Like button(s)")
            
            for btn in like_buttons:
                aria_label = btn.get_attribute('aria-label')
                btn_text = btn.text.strip()
                print(f"  Like button - aria-label: '{aria_label}', text: '{btn_text}'")
                
                # First, try to extract number from button text directly
                if btn_text:
                    import re
                    numbers = re.findall(r'\d+', btn_text)
                    if numbers:
                        likes = int(numbers[0])
                        print(f"  ✓ Extracted likes from button text: {likes}")
                        break
                
                # If not found in button text, check all spans inside
                spans = btn.find_elements(By.TAG_NAME, "span")
                print(f"  Found {len(spans)} span(s) in Like button")
                for span in spans:
                    span_text = span.text.strip()
                    span_class = span.get_attribute('class') or ''
                    print(f"    Like span - text: '{span_text}', class: '{span_class}'")
                    
                    # Check if span text is a number
                    if span_text.isdigit():
                        likes = int(span_text)
                        print(f"    ✓ Extracted likes from span: {likes}")
                        break
                    
                    # Try regex to extract number from span text
                    span_numbers = re.findall(r'\d+', span_text)
                    if span_numbers:
                        likes = int(span_numbers[0])
                        print(f"    ✓ Extracted likes from span (regex): {likes}")
                        break
                
                if likes > 0:
                    break
        except Exception as e:
            print(f"  Error finding Like button: {e}")
        
        # Method 2: Find Comment button with exact aria-label "Comment on this article"
        print("\n=== Searching for 'Comment on this article' button ===")
        try:
            comment_buttons = driver.find_elements(By.XPATH, "//button[@aria-label='Comment on this article']")
            if not comment_buttons:
                # Fallback to contains
                comment_buttons = driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Comment on this article')]")
            
            print(f"Found {len(comment_buttons)} Comment button(s)")
            
            for btn in comment_buttons:
                aria_label = btn.get_attribute('aria-label')
                btn_text = btn.text.strip()
                print(f"  Comment button - aria-label: '{aria_label}', text: '{btn_text}'")
                
                # First, try to extract number from button text directly (this should work based on user's output)
                if btn_text:
                    numbers = re.findall(r'\d+', btn_text)
                    if numbers:
                        comments = int(numbers[0])
                        print(f"  ✓ Extracted comments from button text: {comments}")
                        break
                
                # If not found in button text, check all spans inside
                spans = btn.find_elements(By.TAG_NAME, "span")
                print(f"  Found {len(spans)} span(s) in Comment button")
                for span in spans:
                    span_text = span.text.strip()
                    span_class = span.get_attribute('class') or ''
                    print(f"    Comment span - text: '{span_text}', class: '{span_class}'")
                    
                    # Check if span text is a number
                    if span_text.isdigit():
                        comments = int(span_text)
                        print(f"    ✓ Extracted comments from span: {comments}")
                        break
                    
                    # Try regex to extract number from span text
                    span_numbers = re.findall(r'\d+', span_text)
                    if span_numbers:
                        comments = int(span_numbers[0])
                        print(f"    ✓ Extracted comments from span (regex): {comments}")
                        break
                
                if comments > 0:
                    break
        except Exception as e:
            print(f"  Error finding Comment button: {e}")
        
        driver.quit()
        
        if not likes and not comments:
            error = "Could not find likes or comments in rendered page"
        
    except Exception as e:
        error = str(e)
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        try:
            driver.quit()
        except:
            pass
    
    return likes, comments, error


if __name__ == '__main__':
    url = "https://builder.aws.com/content/35GmxOmEbd0wOvLY6VXe8xFBaD2/introducing-spaces"
    print("="*60)
    print("Selenium-based Blog Scraping Test")
    print("="*60)
    likes, comments, error = scrape_with_selenium(url)
    print(f"\n{'='*60}")
    print(f"RESULT: Likes={likes}, Comments={comments}")
    if error:
        print(f"Error: {error}")
    print(f"{'='*60}")

