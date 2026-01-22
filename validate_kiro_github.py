"""
Kiro GitHub Validator - Command Line Tool
Validates if a GitHub repository contains a .kiro folder in its root directory.

Usage:
    python validate_kiro_github.py <github_url>
    
Examples:
    python validate_kiro_github.py https://github.com/username/repo
    python validate_kiro_github.py https://github.com/username/repo/tree/main
"""
import os
import sys
import re
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fix Windows console encoding for Unicode
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def get_github_headers():
    """Get headers for GitHub API requests"""
    github_token = os.getenv('GITHUB_TOKEN', '').strip()
    
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'Kiro-GitHub-Validator'
    }
    
    if github_token:
        # Try token format first (works for both classic and fine-grained)
        headers['Authorization'] = f'token {github_token}'
    
    return headers, bool(github_token)


def parse_github_url(url):
    """
    Parse GitHub URL and extract owner, repo, and optional branch/path.
    
    Returns: dict with keys: owner, repo, branch (optional), path (optional)
    """
    if not url:
        return None
    
    url = url.strip()
    
    # Remove trailing slashes
    url = url.rstrip('/')
    
    # Pattern 1: https://github.com/owner/repo
    # Pattern 2: https://github.com/owner/repo/tree/branch
    # Pattern 3: https://github.com/owner/repo/tree/branch/path/to/folder
    # Pattern 4: git@github.com:owner/repo.git
    
    patterns = [
        # Standard HTTPS URL with optional tree/branch/path
        r'https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?(?:/tree/([^/]+)(?:/(.+))?)?$',
        # SSH URL
        r'git@github\.com:([^/]+)/([^/]+?)(?:\.git)?$',
    ]
    
    for pattern in patterns:
        match = re.match(pattern, url)
        if match:
            groups = match.groups()
            result = {
                'owner': groups[0],
                'repo': groups[1].replace('.git', ''),
                'branch': groups[2] if len(groups) > 2 else None,
                'path': groups[3] if len(groups) > 3 else None,
                'original_url': url
            }
            return result
    
    return None


def get_default_branch(owner, repo, headers):
    """Get the default branch of a repository"""
    api_url = f'https://api.github.com/repos/{owner}/{repo}'
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('default_branch', 'main')
        elif response.status_code == 404:
            return None  # Repo not found
        else:
            print(f"  ⚠ Warning: Could not fetch repo info (HTTP {response.status_code})")
            return 'main'  # Fallback to main
    except requests.RequestException as e:
        print(f"  ⚠ Warning: Request failed: {e}")
        return 'main'  # Fallback


def get_repo_branches(owner, repo, headers):
    """Get list of branches in the repository"""
    api_url = f'https://api.github.com/repos/{owner}/{repo}/branches'
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            branches = response.json()
            return [b['name'] for b in branches]
        else:
            return []
    except requests.RequestException:
        return []


def check_kiro_at_path(owner, repo, branch, path, headers):
    """
    Check if .kiro folder exists at a specific path.
    
    Returns: dict with 'exists' (bool), 'contents' (list), 'message' (str)
    """
    # Use GitHub Contents API to check the specific path
    if path:
        api_url = f'https://api.github.com/repos/{owner}/{repo}/contents/{path}'
    else:
        api_url = f'https://api.github.com/repos/{owner}/{repo}/contents'
    
    if branch:
        api_url += f'?ref={branch}'
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            contents = response.json()
            
            # Handle case where contents is a single file
            if isinstance(contents, dict):
                return {
                    'exists': False,
                    'contents': [],
                    'message': 'Path is a file, not a directory',
                    'found_at': None
                }
            
            # Look for .kiro folder
            kiro_folder = None
            for item in contents:
                if item['name'] == '.kiro' and item['type'] == 'dir':
                    kiro_folder = item
                    break
            
            if kiro_folder:
                # Found .kiro folder - now check its contents
                kiro_path = f"{path}/.kiro" if path else ".kiro"
                kiro_contents = get_kiro_folder_contents(owner, repo, branch, headers, path)
                location = f'at /{path}' if path else 'in root directory'
                return {
                    'exists': True,
                    'contents': kiro_contents,
                    'message': f'.kiro folder found {location}',
                    'kiro_path': kiro_folder.get('html_url', ''),
                    'found_at': path if path else 'root'
                }
            else:
                # List what's in this path for debugging
                path_items = [f"{item['name']} ({item['type']})" for item in contents[:10]]
                return {
                    'exists': False,
                    'contents': [],
                    'message': None,  # No error, just not found here
                    'path_items': path_items,
                    'found_at': None
                }
        
        elif response.status_code == 404:
            return {
                'exists': False,
                'contents': [],
                'message': None,  # Path not found, will try root
                'found_at': None
            }
        elif response.status_code == 403:
            return {
                'exists': False,
                'contents': [],
                'message': 'Rate limited or access denied',
                'found_at': None
            }
        else:
            return {
                'exists': False,
                'contents': [],
                'message': f'API error: HTTP {response.status_code}',
                'found_at': None
            }
    
    except requests.RequestException as e:
        return {
            'exists': False,
            'contents': [],
            'message': f'Request failed: {str(e)}',
            'found_at': None
        }


def check_kiro_folder_exists(owner, repo, branch, path, headers):
    """
    Check if .kiro folder exists.
    First checks at the given path (if provided), then falls back to root.
    
    Returns: dict with 'exists' (bool), 'contents' (list), 'message' (str)
    """
    # Step 1: If a specific path was provided, check there first
    if path:
        print(f"   Checking path: /{path}")
        result = check_kiro_at_path(owner, repo, branch, path, headers)
        if result['exists']:
            return result  # Found at given path!
        if result['message'] and 'Rate limited' in result['message']:
            return result  # Don't continue if rate limited
        
        # Not found at given path, check root
        print(f"   .kiro not found at /{path}, checking root...")
    
    # Step 2: Check root directory
    result = check_kiro_at_path(owner, repo, branch, None, headers)
    
    if result['exists']:
        return result
    
    # Not found anywhere
    if path:
        result['message'] = f'.kiro folder NOT found at /{path} or in root directory'
        result['root_items'] = result.get('path_items', [])
    else:
        result['message'] = '.kiro folder NOT found in root directory'
        result['root_items'] = result.get('path_items', [])
    
    return result


def get_kiro_folder_contents(owner, repo, branch, headers, path=None):
    """Get contents of the .kiro folder"""
    if path:
        api_url = f'https://api.github.com/repos/{owner}/{repo}/contents/{path}/.kiro'
    else:
        api_url = f'https://api.github.com/repos/{owner}/{repo}/contents/.kiro'
    
    if branch:
        api_url += f'?ref={branch}'
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            contents = response.json()
            if isinstance(contents, list):
                return [{'name': item['name'], 'type': item['type']} for item in contents]
            return []
        else:
            return []
    except requests.RequestException:
        return []


def validate_github_repo(url):
    """
    Main validation function.
    
    Returns: dict with validation results
    """
    print("\n" + "=" * 60)
    print("🔍 Kiro GitHub Repository Validator")
    print("=" * 60)
    
    # Parse the URL
    print(f"\n📎 Input URL: {url}")
    
    parsed = parse_github_url(url)
    if not parsed:
        return {
            'valid': False,
            'error': 'Invalid GitHub URL format',
            'url': url
        }
    
    print(f"   Owner: {parsed['owner']}")
    print(f"   Repo:  {parsed['repo']}")
    if parsed['branch']:
        print(f"   Branch (from URL): {parsed['branch']}")
    if parsed['path']:
        print(f"   Path (from URL): {parsed['path']}")
    
    # Get headers
    headers, has_token = get_github_headers()
    if has_token:
        print("\n🔑 Using GitHub token for authentication")
    else:
        print("\n⚠ No GitHub token - using unauthenticated requests (rate limited)")
    
    owner = parsed['owner']
    repo = parsed['repo']
    
    # Check if repo exists and get branches
    print("\n📂 Checking repository...")
    branches = get_repo_branches(owner, repo, headers)
    
    if not branches:
        # No branches found - could be empty repo or API error
        print("   ⚠ No branches found (repo might be empty or private)")
        
        # Try to check without branch specification
        branch_to_check = parsed.get('branch')
        if not branch_to_check:
            # Use URL as source of truth
            print("   → Using provided URL as source of truth")
            branch_to_check = None
    else:
        print(f"   ✓ Found {len(branches)} branch(es): {', '.join(branches[:5])}")
        if len(branches) > 5:
            print(f"     ... and {len(branches) - 5} more")
        
        # Determine which branch to check
        if parsed.get('branch') and parsed['branch'] in branches:
            branch_to_check = parsed['branch']
            print(f"   → Using branch from URL: {branch_to_check}")
        else:
            # Get default branch
            default_branch = get_default_branch(owner, repo, headers)
            branch_to_check = default_branch
            print(f"   → Using default branch: {branch_to_check}")
    
    # Check for .kiro folder
    path_to_check = parsed.get('path')
    print(f"\n🔎 Checking for .kiro folder...")
    result = check_kiro_folder_exists(owner, repo, branch_to_check, path_to_check, headers)
    
    # Build final result
    validation_result = {
        'valid': result['exists'],
        'url': url,
        'owner': owner,
        'repo': repo,
        'branch': branch_to_check,
        'path': path_to_check,
        'branches_found': branches,
        'kiro_folder': result
    }
    
    # Display results
    print("\n" + "-" * 60)
    print("📋 VALIDATION RESULT")
    print("-" * 60)
    
    if result['exists']:
        found_at = result.get('found_at', 'root')
        print(f"\n✅ VALID - .kiro folder found!")
        if found_at and found_at != 'root':
            print(f"   Location: https://github.com/{owner}/{repo}/tree/{branch_to_check}/{found_at}/.kiro")
        else:
            print(f"   Location: https://github.com/{owner}/{repo}/tree/{branch_to_check}/.kiro")
        
        if result.get('contents'):
            print(f"\n   📁 .kiro folder contents:")
            for item in result['contents']:
                icon = '📄' if item['type'] == 'file' else '📁'
                print(f"      {icon} {item['name']}")
    else:
        print(f"\n❌ INVALID - {result['message']}")
        
        if result.get('root_items'):
            print(f"\n   📁 Root directory contains:")
            for item in result['root_items']:
                print(f"      • {item}")
    
    print("\n" + "=" * 60)
    
    return validation_result


def main():
    """Command line entry point"""
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nError: Please provide a GitHub URL as argument")
        print("\nExample:")
        print("  python validate_kiro_github.py https://github.com/username/repo")
        sys.exit(1)
    
    url = sys.argv[1]
    
    result = validate_github_repo(url)
    
    # Exit with appropriate code
    if result.get('valid'):
        print("\n🎉 Validation PASSED")
        sys.exit(0)
    else:
        print("\n💔 Validation FAILED")
        sys.exit(1)


if __name__ == '__main__':
    main()

