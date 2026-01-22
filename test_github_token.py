"""
Test script to verify GitHub token is working correctly
Run this to check if your GITHUB_TOKEN is valid
"""
import os
import sys
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

def test_github_token():
    """Test if GitHub token is valid"""
    github_token = os.getenv('GITHUB_TOKEN')
    
    if not github_token:
        print("❌ GITHUB_TOKEN not found in .env file!")
        print("\nTo fix:")
        print("1. Create or edit .env file in project root")
        print("2. Add: GITHUB_TOKEN=your_token_here")
        print("3. Get token from: https://github.com/settings/tokens")
        return False
    
    # Strip whitespace
    github_token = github_token.strip()
    
    print(f"✅ Token found: {github_token[:10]}... (length: {len(github_token)})")
    print(f"✅ Token format: {'Valid' if github_token.startswith(('ghp_', 'github_pat_')) else 'Invalid - should start with ghp_ or github_pat_'}")

    # Test with both authentication formats
    headers_token = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'AWS-AI-for-Bharat-Test'
    }
    
    headers_bearer = {
        'Authorization': f'Bearer {github_token}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'AWS-AI-for-Bharat-Test'
    }
    
    # Try token format first (for classic tokens)
    print("\n🔍 Testing with 'token' format...")
    response = requests.get('https://api.github.com/user', headers=headers_token, timeout=10)
    
    if response.status_code == 200:
        user_data = response.json()
        print(f"✅ Token is VALID!")
        print(f"   Authenticated as: {user_data.get('login')}")
        print(f"   Name: {user_data.get('name', 'N/A')}")
        print(f"   Rate limit remaining: {response.headers.get('X-RateLimit-Remaining', 'N/A')}")
        print(f"   Rate limit: {response.headers.get('X-RateLimit-Limit', 'N/A')}")
        return True
    elif response.status_code == 401:
        print(f"❌ Authentication failed (401): Bad credentials")
        
        # Try Bearer format (for fine-grained tokens)
        print("\n🔍 Testing with 'Bearer' format...")
        response = requests.get('https://api.github.com/user', headers=headers_bearer, timeout=10)
        
        if response.status_code == 200:
            user_data = response.json()
            print(f"✅ Token is VALID with Bearer format!")
            print(f"   Authenticated as: {user_data.get('login')}")
            print(f"   Name: {user_data.get('name', 'N/A')}")
            return True
        else:
            print(f"❌ Bearer format also failed: {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Error: {error_data.get('message', 'Unknown error')}")
            except:
                print(f"   Response: {response.text[:200]}")
    else:
        print(f"❌ Unexpected error: {response.status_code}")
        try:
            error_data = response.json()
            print(f"   Error: {error_data.get('message', 'Unknown error')}")
        except:
            print(f"   Response: {response.text[:200]}")
    
    print("\n💡 Troubleshooting:")
    print("1. Check if token is expired - generate new token at https://github.com/settings/tokens")
    print("2. Verify token has 'public_repo' scope (or 'repo' for private repos)")
    print("3. Make sure .env file has no quotes around the token: GITHUB_TOKEN=ghp_xxxxx")
    print("4. Check for extra spaces or newlines in .env file")
    print("5. Restart your Flask application after updating .env")
    
    return False

if __name__ == '__main__':
    print("=" * 60)
    print("GitHub Token Verification Test")
    print("=" * 60)
    print()
    
    success = test_github_token()
    
    print()
    print("=" * 60)
    if success:
        print("✅ Token test PASSED - your token is working!")
    else:
        print("❌ Token test FAILED - please fix the issues above")
    print("=" * 60)

