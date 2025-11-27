"""
Simple test script to debug blog likes and comments scraping
"""
import requests
from bs4 import BeautifulSoup
import re
import sys

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def scrape_blog_metrics(blog_url):
    """
    Scrape likes and comments count from a blog URL
    """
    likes = 0
    comments = 0
    error = None
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        
        print(f"Fetching: {blog_url}")
        response = requests.get(blog_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type')}")
        print(f"Content length: {len(response.text)} chars")
        
        # Save HTML
        with open('blog_page.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print("Saved HTML to blog_page.html\n")
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Method 1: Find buttons with aria-label
        print("=== Method 1: aria-label search ===")
        like_btn = soup.find('button', {'aria-label': re.compile(r'Like', re.I)})
        comment_btn = soup.find('button', {'aria-label': re.compile(r'Comment', re.I)})
        
        if like_btn:
            print(f"Found Like button: {like_btn.get('aria-label')}")
            spans = like_btn.find_all('span')
            for span in spans:
                text = span.get_text(strip=True)
                if text.isdigit():
                    likes = int(text)
                    print(f"  Found likes: {likes}")
        else:
            print("No Like button found")
        
        if comment_btn:
            print(f"Found Comment button: {comment_btn.get('aria-label')}")
            spans = comment_btn.find_all('span')
            for span in spans:
                text = span.get_text(strip=True)
                if text.isdigit():
                    comments = int(text)
                    print(f"  Found comments: {comments}")
        else:
            print("No Comment button found")
        
        # Method 2: Search for _card-action-text spans
        print("\n=== Method 2: _card-action-text search ===")
        action_text_spans = soup.find_all('span', class_=re.compile(r'_card-action-text'))
        print(f"Found {len(action_text_spans)} _card-action-text spans")
        
        for span in action_text_spans:
            parent = span.find_parent('button')
            if parent:
                aria_label = parent.get('aria-label', '')
                text = span.get_text(strip=True)
                print(f"  Button aria-label: '{aria_label}', Text: '{text}'")
                if text.isdigit():
                    if 'like' in aria_label.lower():
                        likes = int(text)
                        print(f"    -> Set likes to {likes}")
                    elif 'comment' in aria_label.lower():
                        comments = int(text)
                        print(f"    -> Set comments to {comments}")
        
        # Method 3: Search all buttons and check their text
        print("\n=== Method 3: All buttons search ===")
        all_buttons = soup.find_all('button')
        print(f"Found {len(all_buttons)} total buttons")
        
        for btn in all_buttons:
            aria_label = btn.get('aria-label', '')
            if 'like' in aria_label.lower() or 'comment' in aria_label.lower():
                print(f"  Button: aria-label='{aria_label}'")
                # Get all text from button
                btn_text = btn.get_text(strip=True)
                print(f"    Full text: '{btn_text}'")
                # Look for numbers
                numbers = re.findall(r'\d+', btn_text)
                if numbers:
                    num = int(numbers[0])
                    if 'like' in aria_label.lower() and not likes:
                        likes = num
                        print(f"    -> Set likes to {likes}")
                    elif 'comment' in aria_label.lower() and not comments:
                        comments = num
                        print(f"    -> Set comments to {comments}")
        
        # Method 4: Search in script tags for JSON data
        print("\n=== Method 4: Script tags search ===")
        scripts = soup.find_all('script')
        print(f"Found {len(scripts)} script tags")
        
        for script in scripts:
            if script.string:
                content = script.string
                # Look for like/comment patterns
                like_matches = re.findall(r'(?:like|likes?)[":\s]*(\d+)', content, re.I)
                comment_matches = re.findall(r'(?:comment|comments?)[":\s]*(\d+)', content, re.I)
                if like_matches or comment_matches:
                    print(f"  Found patterns in script")
                    if like_matches and not likes:
                        likes = int(like_matches[0])
                        print(f"    -> Set likes to {likes}")
                    if comment_matches and not comments:
                        comments = int(comment_matches[0])
                        print(f"    -> Set comments to {comments}")
        
        if not likes and not comments:
            error = "Could not find likes or comments"
            print(f"\n⚠️  {error}")
            print("The page might be JavaScript-rendered. Check blog_page.html for the actual structure.")
        
    except Exception as e:
        error = str(e)
        print(f"\n❌ Error: {error}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print(f"RESULT: Likes={likes}, Comments={comments}")
    if error:
        print(f"Error: {error}")
    print(f"{'='*60}\n")
    
    return likes, comments, error


if __name__ == '__main__':
    url = "https://builder.aws.com/content/35GmxOmEbd0wOvLY6VXe8xFBaD2/introducing-spaces"
    scrape_blog_metrics(url)

