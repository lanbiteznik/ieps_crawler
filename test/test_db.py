import sys
import os
import hashlib
import time
import requests
from urllib.parse import urlparse, urljoin
import psycopg2
import dotenv

# Add parent directory to path so we can import database.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import Database
from crawler import Crawler

# Load environment variables
dotenv.load_dotenv()
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

def test_database_operations():
    """Test all database operations to ensure they work properly"""
    try:
        print("\n===== TESTING DATABASE OPERATIONS =====\n")
        
        # Create instance of Database class
        print("Initializing database connection...")
        # Connect using environment variables
        db = Database()
        print("Database connection successful!")
        
        # 1. Test adding pages to frontier
        print("\n----- Testing frontier operations -----")
        test_urls = [
            "https://med.over.net/test-frontier-1",
            "https://med.over.net/forum/zdravje/test-frontier-2",
            "https://med.over.net/forum/nosecnost-in-otroci/test-frontier-3"
        ]
        
        for url in test_urls:
            success = db.add_page_to_frontier(url)
            print(f"Added URL to frontier: {success} - {url}")
            
        # 2. Test retrieving from frontier using preferential strategy
        next_url = db.get_next_frontier_page_preferential()
        print(f"Retrieved URL from frontier (preferential): {next_url}")
        
        # Mark page as processing
        if next_url:
            success = db.mark_page_as_processing(next_url)
            print(f"Marked URL as processing: {success} - {next_url}")
        
        # 3. Test updating pages (different types)
        print("\n----- Testing page updates -----")
        
        # HTML page
        html_url = test_urls[0]
        html_content = "<html><body><h1>Test HTML</h1><p>This is test content.</p></body></html>"
        content_hash = hashlib.md5(html_content.encode('utf-8')).hexdigest()
        
        page_id = db.update_page_with_hash(html_url, html_content, 200, content_hash)
        print(f"Updated HTML page (ID: {page_id}): {html_url}")
        
        # Binary page
        binary_url = "https://med.over.net/test-binary.pdf"
        db.add_page_to_frontier(binary_url)
        binary_content = b"Fake PDF content for testing"
        binary_page_id = db.update_page(binary_url, None, 200, 'BINARY')
        print(f"Created binary page (ID: {binary_page_id}): {binary_url}")
        
        # Duplicate page
        duplicate_url = "https://med.over.net/test-duplicate"
        db.add_page_to_frontier(duplicate_url)
        success = db.mark_as_duplicate(duplicate_url, html_url)
        print(f"Marked page as duplicate: {success} - {duplicate_url} -> {html_url}")
        
        # 4. Test binary content storage
        print("\n----- Testing binary content storage -----")
        success = db.add_binary_content(binary_page_id, 'PDF', binary_content)
        print(f"Added binary content: {success}")
        
        # 5. Test image extraction
        print("\n----- Testing image storage -----")
        image_filename = "test-image.jpg"
        content_type = "image/jpeg"
        success = db.add_image(page_id, image_filename, content_type, None)
        print(f"Added image: {success} - {image_filename}")
        
        # 6. Test link creation
        print("\n----- Testing link creation -----")
        success = db.add_link(html_url, binary_url)
        print(f"Added link: {success} - {html_url} -> {binary_url}")
        
        # 7. Test content hash checking (for duplicate detection)
        print("\n----- Testing duplicate detection -----")
        duplicate_id, duplicate_url = db.check_content_hash_exists(content_hash)
        print(f"Checking hash existence: {duplicate_id is not None} - ID: {duplicate_id}, URL: {duplicate_url}")
        
        # Test with non-existent hash
        non_existent_hash = "abcdef1234567890"
        result = db.check_content_hash_exists(non_existent_hash)
        print(f"Non-existent hash check: {result[0] is None}")
        
        # 8. Test site operations
        print("\n----- Testing site operations -----")
        domain = "test-domain.com"
        robots_content = "User-agent: *\nDisallow: /private/"
        site_id = db.add_site(domain, robots_content)
        print(f"Added site with robots.txt (ID: {site_id}): {domain}")
        
        # Test updating sitemap
        sitemap_content = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
           <url><loc>http://test-domain.com/page1</loc></url>
           <url><loc>http://test-domain.com/page2</loc></url>
        </urlset>"""
        
        success = db.update_site_sitemap(domain, sitemap_content)
        print(f"Updated site with sitemap: {success} - {domain}")
        
        print("\n===== DATABASE TESTS COMPLETED =====\n")
        
    except Exception as e:
        print(f"Error during testing: {e}")

def test_crawler_extraction():
    """Test the crawler's ability to extract data from web pages"""
    try:
        print("\n===== TESTING CRAWLER EXTRACTION =====\n")
        
        # Create test database and crawler instances
        db = Database()
        crawler = Crawler(seed_urls=["https://med.over.net/"], max_pages=10)
        
        # 1. Test link extraction
        print("\n----- Testing link extraction -----")
        test_html = """
        <html>
            <body>
                <a href="https://med.over.net/forum/test1">Link 1</a>
                <a href="/forum/test2">Relative Link</a>
                <a href="https://example.com/test3">External Link</a>
                <div onclick="window.location.href='/forum/test4'">JavaScript Link</div>
            </body>
        </html>
        """
        base_url = "https://med.over.net/"
        links = crawler.extract_links(test_html, base_url)
        print(f"Links extracted: {len(links)}")
        for link in links:
            print(f"  - {link}")
            
        # Check if extracted links match expected format
        med_links = [link for link in links if "med.over.net" in link]
        print(f"Med.over.net links found: {len(med_links)} of {len(links)}")
        
        # 2. Test image extraction
        print("\n----- Testing image extraction -----")
        test_html_with_images = """
        <html>
            <body>
                <img src="https://med.over.net/wp-content/uploads/test1.jpg" />
                <img src="/wp-content/uploads/test2.png" alt="Test" />
                <div style="background-image: url('/test3.jpg')"></div>
            </body>
        </html>
        """
        
        # First create a test page in the database to store images against
        test_url = "https://med.over.net/test-image-extraction"
        db.add_page_to_frontier(test_url)
        page_id = db.update_page(test_url, test_html_with_images, 200, 'HTML')
        
        # Now extract images
        crawler.extract_images(test_html_with_images, base_url, page_id)
        
        # Query database to check if images were extracted and stored
        cursor = db.conn.cursor()
        cursor.execute("SELECT filename, content_type FROM crawldb.image WHERE page_id = %s", (page_id,))
        images = cursor.fetchall()
        cursor.close()
        
        print(f"Images extracted and stored: {len(images)}")
        for filename, content_type in images:
            print(f"  - {filename} ({content_type})")
        
        # 3. Test binary content detection
        print("\n----- Testing binary content detection -----")
        content_types = [
            "text/html",
            "application/pdf",
            "application/msword",
            "image/jpeg",
            "application/octet-stream"
        ]
        
        for content_type in content_types:
            result = crawler.is_binary_content(content_type)
            print(f"  {content_type} -> Binary: {result is not None} {f'({result})' if result else ''}")
        
        # 4. Test duplicate detection
        print("\n----- Testing duplicate detection -----")
        # Create two pages with same content but different URLs
        content1 = "<html><body>This is duplicate content</body></html>"
        content_hash = crawler.compute_content_hash(content1)
        
        url1 = "https://med.over.net/duplicate-test-1"
        url2 = "https://med.over.net/duplicate-test-2"
        
        db.add_page_to_frontier(url1)
        db.add_page_to_frontier(url2)
        
        # Store first page
        page_id1 = db.update_page_with_hash(url1, content1, 200, content_hash)
        print(f"Created first page with hash: {content_hash}")
        
        # Check if duplicate detection works when crawling second page
        duplicate_id, duplicate_url = db.check_content_hash_exists(content_hash)
        print(f"Duplicate check: Found={duplicate_id is not None}, ID={duplicate_id}, URL={duplicate_url}")
        
        if duplicate_id and duplicate_url == url1:
            print("✅ Duplicate detection successful!")
        else:
            print("❌ Duplicate detection failed!")
        
        print("\n===== CRAWLER EXTRACTION TESTS COMPLETED =====\n")
        
    except Exception as e:
        print(f"Error during testing: {e}")

def test_real_world_extraction():
    """Test crawler against real pages with various content types"""
    try:
        print("\n===== TESTING REAL-WORLD EXTRACTION =====\n")
        
        # Initialize database
        db = Database()
        crawler = Crawler(["https://example.com"], max_pages=1)
        
        # Test different content types with stable websites
        test_cases = [
            # HTML page with many links
            {
                "url": "https://en.wikipedia.org/wiki/Web_crawler",
                "description": "Wikipedia page (rich HTML with many links)",
                "expected_type": "HTML"
            },
            # Image-heavy page
            {
                "url": "https://www.nasa.gov/images/",
                "description": "NASA image gallery (image-heavy page)",
                "expected_type": "HTML"
            },
            # Binary PDF content
            {
                "url": "https://s28.q4cdn.com/392171258/files/doc_downloads/test.pdf",
                "description": "W3C sample PDF file (binary content)",
                "expected_type": "BINARY"
            },
            # JavaScript-heavy page
            {
                "url": "https://github.com/",
                "description": "GitHub homepage (JavaScript-heavy)",
                "expected_type": "HTML"
            }
        ]
        
        for test_case in test_cases:
            url = test_case["url"]
            print(f"\n----- Testing {test_case['description']} -----")
            print(f"URL: {url}")
            
            try:
                # Check if URL is accessible before trying to crawl
                response = requests.head(url, timeout=10)
                if response.status_code != 200:
                    print(f"❌ URL returned status code {response.status_code}. Skipping test.")
                    continue
                    
                # Add page to frontier and reset crawler's visited set for this test
                crawler.visited = set()
                db.add_page_to_frontier(url)
                
                # Crawl the page
                print(f"Crawling page...")
                crawler.crawl_page(url)
                print(f"Crawling completed")
                
                # Check what was extracted
                cursor = db.conn.cursor()
                
                # Check page type
                cursor.execute("SELECT page_type_code, http_status_code FROM crawldb.page WHERE url = %s", (url,))
                page_info = cursor.fetchone()
                
                if page_info:
                    page_type, status = page_info
                    print(f"✅ Page type: {page_type}, HTTP status: {status}")
                    
                    if page_type == test_case["expected_type"]:
                        print(f"✅ Content type matches expected ({test_case['expected_type']})")
                    else:
                        print(f"❌ Content type mismatch: got {page_type}, expected {test_case['expected_type']}")
                    
                    # Get page ID
                    cursor.execute("SELECT id FROM crawldb.page WHERE url = %s", (url,))
                    page_id = cursor.fetchone()[0]
                    
                    # Check specific content based on type
                    if page_type == 'HTML':
                        # Check links
                        cursor.execute("SELECT COUNT(*) FROM crawldb.link WHERE from_page = %s", (page_id,))
                        link_count = cursor.fetchone()[0]
                        print(f"✅ Links extracted: {link_count}")
                        
                        if link_count > 0:
                            cursor.execute("""
                                SELECT p.url FROM crawldb.link l 
                                JOIN crawldb.page p ON l.to_page = p.id
                                WHERE l.from_page = %s LIMIT 3
                            """, (page_id,))
                            sample_links = cursor.fetchall()
                            print("Example links:")
                            for link in sample_links:
                                print(f"  - {link[0]}")
                        
                        # Check images
                        cursor.execute("SELECT COUNT(*) FROM crawldb.image WHERE page_id = %s", (page_id,))
                        image_count = cursor.fetchone()[0]
                        print(f"✅ Images extracted: {image_count}")
                        
                        if image_count > 0:
                            cursor.execute("SELECT filename, content_type FROM crawldb.image WHERE page_id = %s LIMIT 3", (page_id,))
                            sample_images = cursor.fetchall()
                            print("Example images:")
                            for filename, content_type in sample_images:
                                print(f"  - {filename} ({content_type})")
                    
                    elif page_type == 'BINARY':
                        # Check binary content
                        cursor.execute("SELECT COUNT(*) FROM crawldb.page_data WHERE page_id = %s", (page_id,))
                        data_count = cursor.fetchone()[0]
                        print(f"✅ Binary data entries: {data_count}")
                        
                        if data_count > 0:
                            cursor.execute("SELECT data_type_code, data IS NOT NULL FROM crawldb.page_data WHERE page_id = %s", (page_id,))
                            data_info = cursor.fetchone()
                            if data_info:
                                data_type, has_data = data_info
                                print(f"  Binary type: {data_type}, Has data: {has_data}")
                    
                    elif page_type == 'DUPLICATE':
                        # Check duplicate info
                        cursor.execute("""
                            SELECT p2.url FROM crawldb.page p1
                            JOIN crawldb.page p2 ON p1.duplicate_id = p2.id
                            WHERE p1.id = %s
                        """, (page_id,))
                        duplicate_url = cursor.fetchone()
                        if duplicate_url:
                            print(f"✅ Duplicate of: {duplicate_url[0]}")
                else:
                    print(f"❌ Page not found in database: {url}")
                
                cursor.close()
                
            except requests.RequestException as e:
                print(f"❌ Error accessing {url}: {e}")
                continue
            except Exception as e:
                print(f"❌ Error crawling {url}: {e}")
                continue
                
            # Wait 5 seconds between tests to avoid overloading servers
            print("Waiting 5 seconds before next test...")
            time.sleep(5)
        
        # Special test for duplicate content
        print("\n----- Testing duplicate content detection -----")
        # Create a page with known content
        unique_content = f"<html><body>Unique test content {time.time()}</body></html>"
        content_hash = hashlib.md5(unique_content.encode('utf-8')).hexdigest()
        
        url1 = "https://example.com/test-duplicate-1"
        url2 = "https://example.com/test-duplicate-2"
        
        db.add_page_to_frontier(url1)
        db.add_page_to_frontier(url2)
        
        # Add first page
        page_id1 = db.update_page_with_hash(url1, unique_content, 200, content_hash)
        print(f"Created first page with hash: {content_hash}")
        
        # Add second page with same content
        duplicate_id, duplicate_url = db.check_content_hash_exists(content_hash)
        if duplicate_id and duplicate_url == url1:
            print(f"✅ Duplicate detection successful")
            db.mark_as_duplicate(url2, url1)
            print(f"✅ Marked {url2} as duplicate of {url1}")
        else:
            print(f"❌ Duplicate detection failed")
        
        print("\n===== REAL-WORLD TEST COMPLETED =====\n")
        
    except Exception as e:
        print(f"Error during real-world testing: {e}")

if __name__ == "__main__":
    # Comment out these if you only want to run the real-world test
    # test_database_operations()
    # test_crawler_extraction()
    test_real_world_extraction()