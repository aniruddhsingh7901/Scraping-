#!/usr/bin/env python3
"""
test_api.py

Test the Reddit Scraper API
"""

import requests
import json
import time

API_BASE_URL = "http://localhost:8000"


def test_health():
    """Test health endpoint"""
    print("Testing /health endpoint...")
    response = requests.get(f"{API_BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}\n")


def test_accounts_status():
    """Test accounts status endpoint"""
    print("Testing /accounts endpoint...")
    response = requests.get(f"{API_BASE_URL}/accounts")
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Total accounts: {data['total']}")
    print(f"Healthy accounts: {data['healthy']}\n")


def test_scrape_single_subreddit():
    """Test scraping single subreddit"""
    print("Testing scrape with single subreddit...")
    
    payload = {
        "subreddits": ["python"],
        "max_posts": 10,
        "include_comments": True,
        "max_comments_per_post": 50,
        "days_back": 7
    }
    
    print(f"Request: {json.dumps(payload, indent=2)}")
    
    start = time.time()
    response = requests.post(f"{API_BASE_URL}/scrape", json=payload)
    elapsed = time.time() - start
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✓ Success!")
        print(f"Posts scraped: {data['stats']['posts_scraped']}")
        print(f"Comments scraped: {data['stats']['comments_scraped']}")
        print(f"Duration: {elapsed:.2f}s")
        print(f"Account used: {data['stats']['account_used']}")
        print(f"Data entities returned: {len(data['data'])}")
        
        # Show sample data
        if data['data']:
            print(f"\nSample DataEntity:")
            print(json.dumps(data['data'][0], indent=2)[:500] + "...")
    else:
        print(f"Error: {response.text}")
    
    print("\n" + "="*80 + "\n")


def test_scrape_multiple_subreddits():
    """Test scraping multiple subreddits"""
    print("Testing scrape with multiple subreddits...")
    
    payload = {
        "subreddits": ["python", "machinelearning", "datascience"],
        "max_posts": 5,
        "include_comments": True,
        "max_comments_per_post": 20,
        "days_back": 3
    }
    
    print(f"Request: {json.dumps(payload, indent=2)}")
    
    start = time.time()
    response = requests.post(f"{API_BASE_URL}/scrape", json=payload)
    elapsed = time.time() - start
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✓ Success!")
        print(f"Subreddits processed: {data['stats']['subreddits_processed']}")
        print(f"Posts scraped: {data['stats']['posts_scraped']}")
        print(f"Comments scraped: {data['stats']['comments_scraped']}")
        print(f"Duration: {elapsed:.2f}s")
        print(f"Account used: {data['stats']['account_used']}")
        print(f"Throughput: {data['stats']['posts_scraped']/elapsed:.2f} posts/sec")
    else:
        print(f"Error: {response.text}")
    
    print("\n" + "="*80 + "\n")


def test_streaming():
    """Test streaming endpoint"""
    print("Testing /scrape/stream endpoint...")
    
    payload = {
        "subreddits": ["python"],
        "max_posts": 5,
        "include_comments": False,
        "days_back": 1
    }
    
    response = requests.post(
        f"{API_BASE_URL}/scrape/stream",
        json=payload,
        stream=True
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        print("Streaming results:\n")
        count = 0
        for line in response.iter_lines():
            if line:
                decoded = line.decode('utf-8')
                if decoded.startswith('data: '):
                    data = json.loads(decoded[6:])
                    if 'error' in data:
                        print(f"Error: {data['error']}")
                        break
                    else:
                        count += 1
                        print(f"Entity {count}: {data.get('label')} from r/{data.get('subreddit')}")
        
        print(f"\nReceived {count} entities via stream")
    else:
        print(f"Error: {response.text}")
    
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    print("="*80)
    print("Reddit Scraper API - Test Suite")
    print("="*80 + "\n")
    
    try:
        # Test basic endpoints
        test_health()
        test_accounts_status()
        
        # Test scraping
        test_scrape_single_subreddit()
        test_scrape_multiple_subreddits()
        
        # Test streaming
        test_streaming()
        
        print("✓ All tests completed!")
        
    except requests.exceptions.ConnectionError:
        print("❌ Error: Cannot connect to API server at http://localhost:8000")
        print("Make sure the server is running: python reddit_scraper_api.py")
    except Exception as e:
        print(f"❌ Test failed: {e}")