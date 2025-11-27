"""
Test script to debug blog likes and comments scraping
"""
import requests
from bs4 import BeautifulSoup
import re

def scrape_blog_metrics(blog_url):
    """
    Scrape likes and comments count from a blog URL
    Returns: (likes: int, comments: int, error: str or None)
    """
    likes = 0
    comments = 0
    error = None
    
    try:
        # Set headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        print(f"Fetching URL: {blog_url}")
        response = requests.get(blog_url, headers=headers, timeout=15, allow_redirects=True)
        response.raise_for_status()
        print(f"Response status: {response.status_code}")
        print(f"Final URL: {response.url}")
        print(f"Content-Encoding: {response.headers.get('Content-Encoding', 'None')}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'None')}")
        print(f"Content length: {len(response.content)} bytes")
        
        # Check if content is compressed
        if response.headers.get('Content-Encoding') == 'gzip':
            import gzip
            content_text = gzip.decompress(response.content).decode('utf-8', errors='ignore')
            print("Decompressed gzip content")
        elif response.headers.get('Content-Encoding') == 'br':
            import brotli
            content_text = brotli.decompress(response.content).decode('utf-8', errors='ignore')
            print("Decompressed brotli content")
        else:
            # Try to decode normally
            try:
                content_text = response.text
            except:
                content_text = response.content.decode('utf-8', errors='ignore')
        
        # Save raw content first
        with open('blog_page_raw.html', 'wb') as f:
            f.write(response.content)
        print("Saved raw HTML to blog_page_raw.html")
        
        # Try to decode with UTF-8, fallback to latin-1
        try:
            content_text = response.content.decode('utf-8')
        except UnicodeDecodeError:
            content_text = response.content.decode('latin-1', errors='ignore')
        
        # Save decoded text
        with open('blog_page_decoded.txt', 'w', encoding='utf-8', errors='ignore') as f:
            f.write(content_text)
        print("Saved decoded text to blog_page_decoded.txt")
        
        print(f"Content preview (first 1000 chars) - saved to file")
        
        # Check if it's a redirect or minimal HTML (likely JS-rendered)
        if len(content_text) < 2000:
            print("\nWARNING: Page content is very small. This might be a JavaScript-rendered page.")
            print("The likes/comments might be loaded dynamically via JavaScript/API calls.")
        
        # Parse HTML
        soup = BeautifulSoup(content_text, 'html.parser')
        
        # Save HTML for inspection
        with open('blog_page.html', 'w', encoding='utf-8') as f:
            f.write(soup.prettify())
        print("\nSaved HTML to blog_page.html for inspection")
        
        # Look for JSON data in script tags (common in React/Next.js apps)
        print("\n=== Searching for JSON data in script tags ===")
        script_tags = soup.find_all('script')
        for idx, script in enumerate(script_tags):
            script_content = script.string or ''
            if script_content and ('like' in script_content.lower() or 'comment' in script_content.lower()):
                print(f"Script {idx + 1} contains like/comment keywords")
                # Try to find JSON structures
                if '{' in script_content:
                    print(f"  Contains JSON-like structure")
                    # Look for numbers near like/comment
                    like_matches = re.findall(r'(?:like|likes?|liked?)[":\s]*(\d+)', script_content, re.I)
                    comment_matches = re.findall(r'(?:comment|comments?|commented?)[":\s]*(\d+)', script_content, re.I)
                    if like_matches:
                        print(f"  Found like numbers: {like_matches}")
                        if not likes:
                            likes = int(like_matches[0])
                    if comment_matches:
                        print(f"  Found comment numbers: {comment_matches}")
                        if not comments:
                            comments = int(comment_matches[0])
        
        # Find all buttons with aria-label containing "Like"
        print("\n=== Searching for Like button ===")
        like_buttons = soup.find_all('button', {'aria-label': re.compile(r'Like', re.I)})
        print(f"Found {len(like_buttons)} button(s) with 'Like' in aria-label")
        
        for idx, btn in enumerate(like_buttons):
            print(f"\nLike Button {idx + 1}:")
            print(f"  aria-label: {btn.get('aria-label', 'N/A')}")
            print(f"  classes: {btn.get('class', [])}")
            print(f"  Full HTML: {str(btn)[:200]}...")
            
            # Find all spans inside
            spans = btn.find_all('span')
            print(f"  Found {len(spans)} span(s) inside:")
            for span_idx, span in enumerate(spans):
                print(f"    Span {span_idx + 1}:")
                print(f"      classes: {span.get('class', [])}")
                print(f"      text: '{span.get_text(strip=True)}'")
                
                # Check if this span contains the number
                span_text = span.get_text(strip=True)
                if span_text and re.match(r'^\d+$', span_text):
                    try:
                        likes = int(span_text)
                        print(f"      ✓ Found likes: {likes}")
                    except ValueError:
                        pass
        
        # Find all buttons with aria-label containing "Comment"
        print("\n=== Searching for Comment button ===")
        comment_buttons = soup.find_all('button', {'aria-label': re.compile(r'Comment', re.I)})
        print(f"Found {len(comment_buttons)} button(s) with 'Comment' in aria-label")
        
        for idx, btn in enumerate(comment_buttons):
            print(f"\nComment Button {idx + 1}:")
            print(f"  aria-label: {btn.get('aria-label', 'N/A')}")
            print(f"  classes: {btn.get('class', [])}")
            print(f"  Full HTML: {str(btn)[:200]}...")
            
            # Find all spans inside
            spans = btn.find_all('span')
            print(f"  Found {len(spans)} span(s) inside:")
            for span_idx, span in enumerate(spans):
                print(f"    Span {span_idx + 1}:")
                print(f"      classes: {span.get('class', [])}")
                print(f"      text: '{span.get_text(strip=True)}'")
                
                # Check if this span contains the number
                span_text = span.get_text(strip=True)
                if span_text and re.match(r'^\d+$', span_text):
                    try:
                        comments = int(span_text)
                        print(f"      ✓ Found comments: {comments}")
                    except ValueError:
                        pass
        
        # Try alternative: look for buttons with specific class patterns
        print("\n=== Searching for buttons with _card-action class ===")
        action_buttons = soup.find_all('button', class_=re.compile(r'_card-action'))
        print(f"Found {len(action_buttons)} button(s) with '_card-action' class")
        
        for idx, btn in enumerate(action_buttons):
            print(f"\nAction Button {idx + 1}:")
            aria_label = btn.get('aria-label', 'N/A')
            print(f"  aria-label: {aria_label}")
            
            # Find spans with _card-action-text
            text_spans = btn.find_all('span', class_=re.compile(r'_card-action-text'))
            for span in text_spans:
                text = span.get_text(strip=True)
                print(f"  Text span: '{text}'")
                if text and re.match(r'^\d+$', text):
                    if 'like' in aria_label.lower():
                        likes = int(text)
                        print(f"    ✓ Set likes to {likes}")
                    elif 'comment' in aria_label.lower():
                        comments = int(text)
                        print(f"    ✓ Set comments to {comments}")
        
        # Try to find any numeric text near like/comment indicators
        print("\n=== Searching for numeric patterns ===")
        all_text = soup.get_text()
        like_patterns = re.findall(r'(?:like|liked?)\s*:?\s*(\d+)', all_text, re.I)
        comment_patterns = re.findall(r'(?:comment|commented?)\s*:?\s*(\d+)', all_text, re.I)
        
        if like_patterns:
            print(f"Found like patterns: {like_patterns}")
            if not likes:
                likes = int(like_patterns[0])
                print(f"  ✓ Set likes from pattern: {likes}")
        
        if comment_patterns:
            print(f"Found comment patterns: {comment_patterns}")
            if not comments:
                comments = int(comment_patterns[0])
                print(f"  ✓ Set comments from pattern: {comments}")
        
        if not likes and not comments:
            error = "Could not find like or comment counts"
        
    except requests.exceptions.Timeout:
        error = "Request timeout"
        print(f"ERROR: {error}")
    except requests.exceptions.ConnectionError:
        error = "Connection error"
        print(f"ERROR: {error}")
    except requests.exceptions.RequestException as e:
        error = f"Request error: {str(e)}"
        print(f"ERROR: {error}")
    except Exception as e:
        error = f"Error scraping metrics: {str(e)}"
        print(f"ERROR: {error}")
        import traceback
        traceback.print_exc()
    
    print(f"\n=== RESULTS ===")
    print(f"Likes: {likes}")
    print(f"Comments: {comments}")
    print(f"Error: {error}")
    
    return likes, comments, error


if __name__ == '__main__':
    test_url = "https://builder.aws.com/content/35GmxOmEbd0wOvLY6VXe8xFBaD2/introducing-spaces"
    print("=" * 60)
    print("Blog Scraping Test")
    print("=" * 60)
    likes, comments, error = scrape_blog_metrics(test_url)
    print("\n" + "=" * 60)
    print(f"Final Result: Likes={likes}, Comments={comments}, Error={error}")
    print("=" * 60)

