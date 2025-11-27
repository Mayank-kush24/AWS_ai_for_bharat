"""
Quick test to verify Selenium validation is working
"""
import sys
import os

# Add the project directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app_web import scrape_blog_metrics, validate_single_submission
from database import ProjectSubmission

# Test URL
test_url = "https://builder.aws.com/content/35GmxOmEbd0wOvLY6VXe8xFBaD2/introducing-spaces"

print("="*60)
print("Testing Selenium Scraping")
print("="*60)

# Test scrape_blog_metrics directly
print("\n1. Testing scrape_blog_metrics()...")
likes, comments, error, is_404 = scrape_blog_metrics(test_url)
print(f"Results: Likes={likes}, Comments={comments}, Error={error}, Is_404={is_404}")

# Test with a sample submission
print("\n2. Testing validate_single_submission()...")
sample_submission = {
    'workshop_name': 'Test Workshop',
    'email': 'test@example.com',
    'project_link': test_url
}

result = validate_single_submission(sample_submission)
if result:
    print(f"Validation Result: {result}")
else:
    print("Validation returned None")

print("\n" + "="*60)
print("Test Complete")
print("="*60)

