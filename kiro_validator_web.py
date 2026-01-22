"""
Kiro GitHub Validator - Standalone Web Application
Upload CSV with email and GitHub links to validate .kiro folders in bulk.

Run with: python kiro_validator_web.py
Access at: http://localhost:5001
"""
import os
import sys
import csv
import io
import re
import json
import time
import requests
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify, Response
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = 'kiro-validator-secret-key'

# Configuration
MAX_WORKERS = 10  # Concurrent validation threads
RATE_LIMIT_DELAY = 0.1  # Reduced delay since we're using authenticated requests


def get_github_headers():
    """Get headers for GitHub API requests"""
    github_token = os.getenv('GITHUB_TOKEN', '').strip()
    
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'Kiro-GitHub-Validator'
    }
    
    if github_token:
        headers['Authorization'] = f'token {github_token}'
    
    return headers, bool(github_token)


def parse_github_url(url):
    """Parse GitHub URL and extract owner, repo, and optional branch"""
    if not url:
        return None
    
    url = url.strip().rstrip('/')
    
    patterns = [
        r'https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?(?:/tree/([^/]+)(?:/(.+))?)?$',
        r'git@github\.com:([^/]+)/([^/]+?)(?:\.git)?$',
    ]
    
    for pattern in patterns:
        match = re.match(pattern, url)
        if match:
            groups = match.groups()
            return {
                'owner': groups[0],
                'repo': groups[1].replace('.git', ''),
                'branch': groups[2] if len(groups) > 2 else None,
                'path': groups[3] if len(groups) > 3 else None,
                'original_url': url
            }
    
    return None


def get_default_branch(owner, repo, headers):
    """Get the default branch of a repository"""
    api_url = f'https://api.github.com/repos/{owner}/{repo}'
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json().get('default_branch', 'main')
        return None
    except:
        return 'main'


def check_kiro_folder_at_path(owner, repo, branch, path, headers):
    """Check if .kiro folder exists at a specific path"""
    # Build API URL for the specific path
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
            # Handle case where contents is a single file (not a directory)
            if isinstance(contents, dict):
                return False, 'Path is a file, not a directory'
            
            for item in contents:
                if item['name'] == '.kiro' and item['type'] == 'dir':
                    location = f'at /{path}' if path else 'in root'
                    return True, f'Found .kiro folder {location}'
            return False, None  # Not found at this path, but no error
        elif response.status_code == 404:
            return False, None  # Path not found, try root
        elif response.status_code == 403:
            return False, 'Rate limited or access denied'
        else:
            return False, f'API error: {response.status_code}'
    except Exception as e:
        return False, f'Request failed: {str(e)}'


def check_kiro_folder(owner, repo, branch, path, headers):
    """
    Check if .kiro folder exists.
    First checks at the given path (if provided), then falls back to root.
    """
    # Step 1: If a specific path was provided, check there first
    if path:
        valid, reason = check_kiro_folder_at_path(owner, repo, branch, path, headers)
        if valid:
            return True, reason  # Found at given path!
        if reason and 'Rate limited' in reason:
            return False, reason  # Don't continue if rate limited
    
    # Step 2: Only check root if not found at given path (or no path provided)
    if path:
        # Path was provided but .kiro not found there, now check root
        valid, reason = check_kiro_folder_at_path(owner, repo, branch, None, headers)
        if valid:
            return True, reason
        if reason:
            return False, reason
        return False, f'No .kiro folder at /{path} or in root'
    else:
        # No path provided, just check root
        valid, reason = check_kiro_folder_at_path(owner, repo, branch, None, headers)
        if valid:
            return True, reason
        if reason:
            return False, reason
        return False, 'No .kiro folder in root'


def validate_single_repo(email, github_url, headers, apply_delay=True):
    """Validate a single repository"""
    result = {
        'email': email,
        'github_url': github_url,
        'valid': False,
        'reason': '',
        'owner': '',
        'repo': '',
        'branch': '',
        'path': ''
    }
    
    if not github_url or not github_url.strip():
        result['reason'] = 'No GitHub URL provided'
        return result
    
    parsed = parse_github_url(github_url)
    if not parsed:
        result['reason'] = 'Invalid GitHub URL format'
        return result
    
    result['owner'] = parsed['owner']
    result['repo'] = parsed['repo']
    result['path'] = parsed.get('path', '')
    
    # Get branch
    if parsed.get('branch'):
        branch = parsed['branch']
    else:
        branch = get_default_branch(parsed['owner'], parsed['repo'], headers)
        if not branch:
            result['reason'] = 'Could not determine default branch'
            return result
    
    result['branch'] = branch
    
    # Check for .kiro folder (minimal delay for parallel processing)
    if apply_delay:
        time.sleep(RATE_LIMIT_DELAY)
    
    # First check at given path, then fall back to root if needed
    path = parsed.get('path')
    valid, reason = check_kiro_folder(parsed['owner'], parsed['repo'], branch, path, headers)
    
    result['valid'] = valid
    result['reason'] = reason
    
    return result


# HTML Template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kiro GitHub Validator</title>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0a0f;
            --bg-secondary: #12121a;
            --bg-tertiary: #1a1a25;
            --accent-primary: #00d4aa;
            --accent-secondary: #7c3aed;
            --accent-warning: #f59e0b;
            --accent-error: #ef4444;
            --text-primary: #e4e4e7;
            --text-secondary: #a1a1aa;
            --text-muted: #71717a;
            --border-color: #27272a;
            --success-bg: rgba(0, 212, 170, 0.1);
            --error-bg: rgba(239, 68, 68, 0.1);
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Space Grotesk', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            background-image: 
                radial-gradient(ellipse at 20% 0%, rgba(124, 58, 237, 0.15) 0%, transparent 50%),
                radial-gradient(ellipse at 80% 100%, rgba(0, 212, 170, 0.1) 0%, transparent 50%);
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        header {
            text-align: center;
            margin-bottom: 3rem;
            padding: 2rem 0;
        }
        
        h1 {
            font-size: 2.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 0.5rem;
        }
        
        .subtitle {
            color: var(--text-secondary);
            font-size: 1.1rem;
        }
        
        .card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 2rem;
            margin-bottom: 2rem;
        }
        
        .card-title {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        
        .card-title .icon {
            width: 32px;
            height: 32px;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1rem;
        }
        
        .upload-zone {
            border: 2px dashed var(--border-color);
            border-radius: 12px;
            padding: 3rem 2rem;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s ease;
            background: var(--bg-tertiary);
        }
        
        .upload-zone:hover, .upload-zone.dragover {
            border-color: var(--accent-primary);
            background: rgba(0, 212, 170, 0.05);
        }
        
        .upload-zone .icon {
            font-size: 3rem;
            margin-bottom: 1rem;
        }
        
        .upload-zone p {
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }
        
        .upload-zone .formats {
            font-size: 0.875rem;
            color: var(--text-muted);
        }
        
        .file-input {
            display: none;
        }
        
        .file-selected {
            margin-top: 1rem;
            padding: 1rem;
            background: var(--bg-primary);
            border-radius: 8px;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        
        .file-selected .name {
            flex: 1;
            font-family: 'JetBrains Mono', monospace;
            color: var(--accent-primary);
        }
        
        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            padding: 0.875rem 1.75rem;
            border: none;
            border-radius: 8px;
            font-family: 'Space Grotesk', sans-serif;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s ease;
        }
        
        .btn-primary {
            background: linear-gradient(135deg, var(--accent-primary), #00b894);
            color: var(--bg-primary);
        }
        
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(0, 212, 170, 0.3);
        }
        
        .btn-primary:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }
        
        .btn-secondary {
            background: var(--bg-tertiary);
            color: var(--text-primary);
            border: 1px solid var(--border-color);
        }
        
        .btn-secondary:hover {
            background: var(--border-color);
        }
        
        .actions {
            display: flex;
            gap: 1rem;
            margin-top: 1.5rem;
        }
        
        .progress-section {
            display: none;
        }
        
        .progress-section.active {
            display: block;
        }
        
        .progress-bar-container {
            background: var(--bg-tertiary);
            border-radius: 8px;
            height: 8px;
            overflow: hidden;
            margin-bottom: 1rem;
        }
        
        .progress-bar {
            height: 100%;
            background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary));
            width: 0%;
            transition: width 0.3s ease;
        }
        
        .progress-text {
            display: flex;
            justify-content: space-between;
            color: var(--text-secondary);
            font-size: 0.875rem;
        }
        
        .results-section {
            display: none;
        }
        
        .results-section.active {
            display: block;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }
        
        .stat-card {
            background: var(--bg-tertiary);
            border-radius: 12px;
            padding: 1.5rem;
            text-align: center;
        }
        
        .stat-value {
            font-size: 2rem;
            font-weight: 700;
            font-family: 'JetBrains Mono', monospace;
        }
        
        .stat-value.valid {
            color: var(--accent-primary);
        }
        
        .stat-value.invalid {
            color: var(--accent-error);
        }
        
        .stat-value.total {
            color: var(--accent-secondary);
        }
        
        .stat-label {
            color: var(--text-muted);
            font-size: 0.875rem;
            margin-top: 0.25rem;
        }
        
        .results-table-container {
            overflow-x: auto;
            border-radius: 12px;
            border: 1px solid var(--border-color);
        }
        
        .results-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        
        .results-table th {
            background: var(--bg-tertiary);
            padding: 1rem;
            text-align: left;
            font-weight: 600;
            color: var(--text-secondary);
            border-bottom: 1px solid var(--border-color);
            position: sticky;
            top: 0;
        }
        
        .results-table td {
            padding: 1rem;
            border-bottom: 1px solid var(--border-color);
            vertical-align: middle;
        }
        
        .results-table tr:last-child td {
            border-bottom: none;
        }
        
        .results-table tr:hover {
            background: var(--bg-tertiary);
        }
        
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.375rem;
            padding: 0.375rem 0.75rem;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
        }
        
        .status-badge.valid {
            background: var(--success-bg);
            color: var(--accent-primary);
        }
        
        .status-badge.invalid {
            background: var(--error-bg);
            color: var(--accent-error);
        }
        
        .email-cell {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
        }
        
        .github-link {
            color: var(--accent-secondary);
            text-decoration: none;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.8rem;
            word-break: break-all;
        }
        
        .github-link:hover {
            text-decoration: underline;
        }
        
        .reason-cell {
            color: var(--text-muted);
            font-size: 0.85rem;
        }
        
        .filter-tabs {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1.5rem;
        }
        
        .filter-tab {
            padding: 0.5rem 1rem;
            border-radius: 6px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 0.875rem;
            transition: all 0.2s ease;
        }
        
        .filter-tab:hover, .filter-tab.active {
            background: var(--accent-primary);
            color: var(--bg-primary);
            border-color: var(--accent-primary);
        }
        
        .csv-format {
            background: var(--bg-tertiary);
            border-radius: 8px;
            padding: 1rem;
            margin-top: 1rem;
        }
        
        .csv-format code {
            font-family: 'JetBrains Mono', monospace;
            color: var(--accent-primary);
            font-size: 0.875rem;
        }
        
        .csv-format p {
            color: var(--text-muted);
            font-size: 0.875rem;
            margin-bottom: 0.5rem;
        }
        
        .token-status {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 1rem;
            border-radius: 20px;
            font-size: 0.875rem;
            margin-bottom: 1rem;
        }
        
        .token-status.active {
            background: var(--success-bg);
            color: var(--accent-primary);
        }
        
        .token-status.inactive {
            background: var(--error-bg);
            color: var(--accent-error);
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        .spinner {
            width: 20px;
            height: 20px;
            border: 2px solid var(--bg-primary);
            border-top-color: transparent;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔍 Kiro GitHub Validator</h1>
            <p class="subtitle">Bulk validate GitHub repositories for .kiro folder</p>
            <div class="token-status {{ 'active' if has_token else 'inactive' }}">
                {{ '🔑 GitHub Token Active' if has_token else '⚠️ No GitHub Token (Rate Limited)' }}
            </div>
        </header>
        
        <div class="card" id="uploadCard">
            <div class="card-title">
                <span class="icon">📁</span>
                Upload CSV File
            </div>
            
            <div class="upload-zone" id="uploadZone" onclick="document.getElementById('fileInput').click()">
                <div class="icon">📤</div>
                <p><strong>Drag & drop your CSV file here</strong></p>
                <p>or click to browse</p>
                <p class="formats">Supported: CSV files with email and github_link columns</p>
            </div>
            
            <input type="file" id="fileInput" class="file-input" accept=".csv">
            
            <div class="file-selected" id="fileSelected" style="display: none;">
                <span>📄</span>
                <span class="name" id="fileName"></span>
                <span id="rowCount"></span>
            </div>
            
            <div class="csv-format">
                <p>Expected CSV format:</p>
                <code>email,github_link<br>user@example.com,https://github.com/user/repo</code>
            </div>
            
            <div class="actions">
                <button class="btn btn-primary" id="validateBtn" disabled onclick="startValidation()">
                    🚀 Start Validation
                </button>
                <button class="btn btn-secondary" id="resetBtn" style="display: none;" onclick="resetForm()">
                    🔄 Reset
                </button>
            </div>
        </div>
        
        <div class="card progress-section" id="progressSection">
            <div class="card-title">
                <span class="icon">⏳</span>
                Validation Progress
            </div>
            
            <div class="progress-bar-container">
                <div class="progress-bar" id="progressBar"></div>
            </div>
            
            <div class="progress-text">
                <span id="progressText">Processing...</span>
                <span id="progressPercent">0%</span>
            </div>
        </div>
        
        <div class="card results-section" id="resultsSection">
            <div class="card-title">
                <span class="icon">📊</span>
                Validation Results
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value total" id="totalCount">0</div>
                    <div class="stat-label">Total</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value valid" id="validCount">0</div>
                    <div class="stat-label">Valid</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value invalid" id="invalidCount">0</div>
                    <div class="stat-label">Invalid</div>
                </div>
            </div>
            
            <div class="filter-tabs">
                <button class="filter-tab active" onclick="filterResults('all')">All</button>
                <button class="filter-tab" onclick="filterResults('valid')">✓ Valid Only</button>
                <button class="filter-tab" onclick="filterResults('invalid')">✗ Invalid Only</button>
            </div>
            
            <div class="actions" style="margin-bottom: 1.5rem;">
                <button class="btn btn-secondary" onclick="downloadResults('all')">📥 Download All (CSV)</button>
                <button class="btn btn-secondary" onclick="downloadResults('valid')">📥 Download Valid</button>
                <button class="btn btn-secondary" onclick="downloadResults('invalid')">📥 Download Invalid</button>
            </div>
            
            <div class="results-table-container" style="max-height: 500px; overflow-y: auto;">
                <table class="results-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Status</th>
                            <th>Email</th>
                            <th>GitHub URL</th>
                            <th>Reason</th>
                        </tr>
                    </thead>
                    <tbody id="resultsBody">
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <script>
        let csvData = [];
        let validationResults = [];
        let currentFilter = 'all';
        
        // Drag and drop handling
        const uploadZone = document.getElementById('uploadZone');
        const fileInput = document.getElementById('fileInput');
        
        uploadZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadZone.classList.add('dragover');
        });
        
        uploadZone.addEventListener('dragleave', () => {
            uploadZone.classList.remove('dragover');
        });
        
        uploadZone.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadZone.classList.remove('dragover');
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                handleFile(files[0]);
            }
        });
        
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                handleFile(e.target.files[0]);
            }
        });
        
        function handleFile(file) {
            if (!file.name.endsWith('.csv')) {
                alert('Please upload a CSV file');
                return;
            }
            
            const reader = new FileReader();
            reader.onload = (e) => {
                parseCSV(e.target.result, file.name);
            };
            reader.readAsText(file);
        }
        
        function parseCSV(content, fileName) {
            const lines = content.trim().split('\\n');
            if (lines.length < 2) {
                alert('CSV file must have a header row and at least one data row');
                return;
            }
            
            // Parse header
            const header = lines[0].split(',').map(h => h.trim().toLowerCase().replace(/['"]/g, ''));
            const emailIdx = header.findIndex(h => h.includes('email'));
            const githubIdx = header.findIndex(h => h.includes('github') || h.includes('link') || h.includes('url'));
            
            if (emailIdx === -1 || githubIdx === -1) {
                alert('CSV must have columns containing "email" and "github" (or "link"/"url")');
                return;
            }
            
            // Parse data rows
            csvData = [];
            for (let i = 1; i < lines.length; i++) {
                const row = parseCSVLine(lines[i]);
                if (row.length > Math.max(emailIdx, githubIdx)) {
                    const email = row[emailIdx].trim();
                    const github = row[githubIdx].trim();
                    if (email || github) {
                        csvData.push({ email, github_link: github });
                    }
                }
            }
            
            if (csvData.length === 0) {
                alert('No valid data rows found in CSV');
                return;
            }
            
            // Update UI
            document.getElementById('fileSelected').style.display = 'flex';
            document.getElementById('fileName').textContent = fileName;
            document.getElementById('rowCount').textContent = `(${csvData.length} rows)`;
            document.getElementById('validateBtn').disabled = false;
            document.getElementById('resetBtn').style.display = 'inline-flex';
        }
        
        function parseCSVLine(line) {
            const result = [];
            let current = '';
            let inQuotes = false;
            
            for (let i = 0; i < line.length; i++) {
                const char = line[i];
                if (char === '"') {
                    inQuotes = !inQuotes;
                } else if (char === ',' && !inQuotes) {
                    result.push(current.trim());
                    current = '';
                } else {
                    current += char;
                }
            }
            result.push(current.trim());
            return result;
        }
        
        async function startValidation() {
            if (csvData.length === 0) return;
            
            // Show progress
            document.getElementById('progressSection').classList.add('active');
            document.getElementById('resultsSection').classList.remove('active');
            document.getElementById('validateBtn').disabled = true;
            
            validationResults = [];
            const total = csvData.length;
            let processed = 0;
            
            // Process in batches via API
            const response = await fetch('/api/validate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ data: csvData })
            });
            
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            
            while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                
                const text = decoder.decode(value);
                const lines = text.split('\\n').filter(l => l.trim());
                
                for (const line of lines) {
                    try {
                        const result = JSON.parse(line);
                        if (result.type === 'progress') {
                            processed = result.processed;
                            updateProgress(processed, total);
                        } else if (result.type === 'result') {
                            validationResults.push(result.data);
                        } else if (result.type === 'complete') {
                            showResults();
                        }
                    } catch (e) {
                        console.error('Parse error:', e);
                    }
                }
            }
        }
        
        function updateProgress(processed, total) {
            const percent = Math.round((processed / total) * 100);
            document.getElementById('progressBar').style.width = percent + '%';
            document.getElementById('progressPercent').textContent = percent + '%';
            document.getElementById('progressText').textContent = `Processing ${processed} of ${total}...`;
        }
        
        function showResults() {
            document.getElementById('progressSection').classList.remove('active');
            document.getElementById('resultsSection').classList.add('active');
            document.getElementById('validateBtn').disabled = false;
            
            const valid = validationResults.filter(r => r.valid).length;
            const invalid = validationResults.length - valid;
            
            document.getElementById('totalCount').textContent = validationResults.length;
            document.getElementById('validCount').textContent = valid;
            document.getElementById('invalidCount').textContent = invalid;
            
            renderResults();
        }
        
        function filterResults(filter) {
            currentFilter = filter;
            document.querySelectorAll('.filter-tab').forEach(t => t.classList.remove('active'));
            event.target.classList.add('active');
            renderResults();
        }
        
        function renderResults() {
            const tbody = document.getElementById('resultsBody');
            let filtered = validationResults;
            
            if (currentFilter === 'valid') {
                filtered = validationResults.filter(r => r.valid);
            } else if (currentFilter === 'invalid') {
                filtered = validationResults.filter(r => !r.valid);
            }
            
            tbody.innerHTML = filtered.map((r, i) => `
                <tr>
                    <td>${i + 1}</td>
                    <td>
                        <span class="status-badge ${r.valid ? 'valid' : 'invalid'}">
                            ${r.valid ? '✓ Valid' : '✗ Invalid'}
                        </span>
                    </td>
                    <td class="email-cell">${escapeHtml(r.email)}</td>
                    <td>
                        <a href="${escapeHtml(r.github_url)}" target="_blank" class="github-link">
                            ${escapeHtml(r.github_url || 'N/A')}
                        </a>
                    </td>
                    <td class="reason-cell">${escapeHtml(r.reason)}</td>
                </tr>
            `).join('');
        }
        
        function escapeHtml(text) {
            if (!text) return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        function downloadResults(filter) {
            let data = validationResults;
            if (filter === 'valid') {
                data = validationResults.filter(r => r.valid);
            } else if (filter === 'invalid') {
                data = validationResults.filter(r => !r.valid);
            }
            
            const csv = 'email,github_url,valid,reason,owner,repo,branch\\n' + 
                data.map(r => 
                    `"${r.email}","${r.github_url}",${r.valid},"${r.reason}","${r.owner}","${r.repo}","${r.branch}"`
                ).join('\\n');
            
            const blob = new Blob([csv], { type: 'text/csv' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `kiro_validation_${filter}_${new Date().toISOString().slice(0,10)}.csv`;
            a.click();
            URL.revokeObjectURL(url);
        }
        
        function resetForm() {
            csvData = [];
            validationResults = [];
            document.getElementById('fileInput').value = '';
            document.getElementById('fileSelected').style.display = 'none';
            document.getElementById('validateBtn').disabled = true;
            document.getElementById('resetBtn').style.display = 'none';
            document.getElementById('progressSection').classList.remove('active');
            document.getElementById('resultsSection').classList.remove('active');
        }
    </script>
</body>
</html>
'''


@app.route('/')
def index():
    """Main page"""
    _, has_token = get_github_headers()
    return render_template_string(HTML_TEMPLATE, has_token=has_token)


@app.route('/api/validate', methods=['POST'])
def validate_api():
    """Streaming validation endpoint with parallel processing"""
    data = request.json.get('data', [])
    headers, has_token = get_github_headers()
    
    # Adjust workers based on whether we have a token (higher rate limits)
    num_workers = MAX_WORKERS if has_token else 3
    
    def generate():
        import queue
        import threading
        
        results_queue = queue.Queue()
        total = len(data)
        processed_count = [0]  # Use list for mutable reference in closure
        lock = threading.Lock()
        
        def validate_worker(index, item):
            """Worker function for parallel validation"""
            email = item.get('email', '')
            github_url = item.get('github_link', '')
            
            # Skip delay when using token (high rate limit) and parallel processing
            result = validate_single_repo(email, github_url, headers, apply_delay=not has_token)
            result['_index'] = index  # Track original order
            
            with lock:
                processed_count[0] += 1
                current_count = processed_count[0]
            
            results_queue.put(('result', result, current_count))
        
        def run_parallel():
            """Run all validations in parallel"""
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                # Submit all tasks
                futures = {
                    executor.submit(validate_worker, i, item): i 
                    for i, item in enumerate(data)
                }
                
                # Wait for all to complete
                for future in as_completed(futures):
                    try:
                        future.result()  # Raise any exceptions
                    except Exception as e:
                        print(f"Validation error: {e}")
            
            # Signal completion
            results_queue.put(('complete', None, total))
        
        # Start parallel processing in a separate thread
        worker_thread = threading.Thread(target=run_parallel)
        worker_thread.start()
        
        # Stream results as they come in
        completed = False
        while not completed:
            try:
                msg_type, result, count = results_queue.get(timeout=30)
                
                if msg_type == 'result':
                    # Send progress update
                    yield json.dumps({'type': 'progress', 'processed': count}) + '\n'
                    # Send result
                    yield json.dumps({'type': 'result', 'data': result}) + '\n'
                elif msg_type == 'complete':
                    completed = True
                    yield json.dumps({'type': 'complete'}) + '\n'
            except queue.Empty:
                # Timeout - check if thread is still alive
                if not worker_thread.is_alive():
                    completed = True
                    yield json.dumps({'type': 'complete'}) + '\n'
        
        worker_thread.join(timeout=5)
    
    return Response(generate(), mimetype='application/x-ndjson')


if __name__ == '__main__':
    print("=" * 60)
    print("🔍 Kiro GitHub Validator - Web Application")
    print("=" * 60)
    print()
    print("Starting server at http://localhost:5001")
    print("Press Ctrl+C to stop")
    print()
    
    _, has_token = get_github_headers()
    if has_token:
        print("✅ GitHub token detected - using authenticated requests")
        print(f"⚡ Parallel processing enabled: {MAX_WORKERS} concurrent workers")
    else:
        print("⚠️  No GitHub token - rate limiting may apply")
        print("   Set GITHUB_TOKEN in .env file for higher limits")
        print("⚡ Parallel processing enabled: 3 concurrent workers (limited)")
    print()
    
    app.run(host='0.0.0.0', port=5001, debug=True)

