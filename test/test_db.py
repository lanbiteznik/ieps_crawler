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

from oldstufffrompa1.database import Database
from oldstufffrompa1.crawler import Crawler

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
        
        page_id = db.update_page_with_hash_and_minhash(html_url, html_content, 200, content_hash)
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
        page_id1 = db.update_page_with_hash_and_minhash(url1, content1, 200, content_hash)
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
        
        # Updated test cases with more reliable URLs and better error handling
        test_cases = [
            # HTML page - FRI website (both domains to try)
            {
                "url": "https://fri.uni-lj.si/",
                "description": "FRI homepage (standard HTML)",
                "expected_type": "HTML"
            },
            # HTML page with many links - Wikipedia is very stable
            {
                "url": "https://en.wikipedia.org/wiki/Web_crawler",
                "description": "Wikipedia page (rich HTML with many links)",
                "expected_type": "HTML"
            },
            # Forum page - critical for validation requirements
            {
                "url": "https://med.over.net/forum/",
                "description": "Med.over.net forum (rich content structure)",
                "expected_type": "HTML"
            },
            # Binary PDF content - multiple reliable sources
            {
                "url": "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf",
                "description": "W3C test PDF file (binary content)",
                "expected_type": "BINARY"
            },
            # Backup PDF in case the first one fails
            {
                "url": "https://pdfobject.com/pdf/sample.pdf", 
                "description": "Alternative sample PDF file",
                "expected_type": "BINARY"
            },
            # Image-heavy page
            {
                "url": "https://fri.uni-lj.si/sl/galerija",
                "description": "FRI gallery (image-heavy page)",
                "expected_type": "HTML"
            }
        ]
        
        for test_case in test_cases:
            url = test_case["url"]
            print(f"\n----- Testing {test_case['description']} -----")
            print(f"URL: {url}")
            
            try:
                # Adding proper headers to avoid being blocked
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                # Check if URL is accessible before trying to crawl
                response = requests.get(
                    url, 
                    timeout=15,  # Increased timeout for reliability
                    stream=True,
                    allow_redirects=True,
                    headers=headers
                )
                
                if response.status_code != 200:
                    print(f"❌ URL returned status code {response.status_code}. Skipping test.")
                    continue
                
                # Get content type before closing
                content_type = response.headers.get('Content-Type', '')
                print(f"Content-Type: {content_type}")
                
                # Stop the response download since we just needed the status code
                response.close()
                
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
        page_id1 = db.update_page_with_hash_and_minhash(url1, unique_content, 200, content_hash)
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

def test_duplicate_detection():
    """Test duplicate content detection with real-world examples"""
    try:
        print("\n===== TESTING DUPLICATE DETECTION =====\n")
        
        db = Database()
        crawler = Crawler(["https://example.com"], max_pages=1)
        
        # 1. Real-world duplicate pairs - pages that commonly have identical content
        duplicate_pairs = [
            # University pages often have identical content on different paths
           
            # Wikipedia pages with redirects
            ("https://en.wikipedia.org/wiki/Computer_Science", 
             "https://en.wikipedia.org/wiki/Computer_science")
        ]
        
        # 2. Generate unique content hash that won't conflict with existing pages
        unique_content = f"<html><body>Test duplicate content {time.time()}</body></html>"
        content_hash = crawler.compute_content_hash(unique_content)
        
        # 3. For testing, create controlled duplicate content
        print("\nTesting with controlled content:")
        url1 = f"https://test-dup-{int(time.time())}-1.example.com/"
        url2 = f"https://test-dup-{int(time.time())}-2.example.com/"
        
        print(f"Test URL 1: {url1}")
        print(f"Test URL 2: {url2}")
        
        # First ensure any old test pages are cleaned up
        # This prevents foreign key constraint issues
        cleanup_urls = [url1, url2]
        cursor = db.conn.cursor()
        
        for url in cleanup_urls:
            try:
                # First get page ID
                cursor.execute("SELECT id FROM crawldb.page WHERE url = %s", (url,))
                result = cursor.fetchone()
                
                if result:
                    page_id = result[0]
                    
                    # Delete links first
                    cursor.execute(
                        "DELETE FROM crawldb.link WHERE from_page = %s OR to_page = %s", 
                        (page_id, page_id)
                    )
                    
                    # Then delete page
                    cursor.execute("DELETE FROM crawldb.page WHERE id = %s", (page_id,))
            except Exception as e:
                # Non-critical error
                pass
        
        db.conn.commit()
        cursor.close()
        
        # Add to frontier and create first page
        db.add_page_to_frontier(url1)
        page_id1 = db.update_page_with_hash_and_minhash(url1, unique_content, 200, content_hash)
        print(f"Created first page with hash: {content_hash}")
        
        # Check for duplicate detection
        db.add_page_to_frontier(url2)
        duplicate_id, duplicate_url = db.check_content_hash_exists(content_hash)
        
        if duplicate_id and duplicate_url == url1:
            print("✅ Duplicate detection successful")
            # Mark as duplicate - this should set page_type_code='DUPLICATE'
            success = db.mark_as_duplicate(url2, url1)
            print(f"Marked as duplicate: {success}")
            
            # Verify in database using page_type_code
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT page_type_code FROM crawldb.page WHERE url = %s", 
                (url2,)
            )
            result = cursor.fetchone()
            if result and result[0] == 'DUPLICATE':
                print("✅ Database successfully marked page as duplicate")
                
                # Additional check for successful crawler integration
                db.update_page_with_hash_and_minhash(url2, unique_content, 200, content_hash)
                cursor.execute(
                    "SELECT page_type_code FROM crawldb.page WHERE url = %s", 
                    (url2,)
                )
                verify = cursor.fetchone()
                if verify and verify[0] == 'DUPLICATE':
                    print("✅ Duplicate detection persists through page updates")
                else:
                    print("❌ Page duplicate status was incorrectly changed")
            else:
                print("❌ Page not properly marked as duplicate")
            cursor.close()
        else:
            print("❌ Duplicate detection failed")
        
        # Clean up test data properly
        clean_test_data(db, [url1, url2])
        
        print("\n===== DUPLICATE DETECTION TEST COMPLETED =====\n")
        
    except Exception as e:
        print(f"Error during duplicate detection testing: {e}")

def clean_test_data(db, urls):
    """Helper method to properly clean test data respecting foreign keys"""
    cursor = db.conn.cursor()
    try:
        # Get all related page IDs
        cursor.execute(
            "SELECT id FROM crawldb.page WHERE url IN %s",
            (tuple(urls),)
        )
        page_ids = [row[0] for row in cursor.fetchall()]
        
        if page_ids:
            # Format for SQL IN clause
            if len(page_ids) == 1:
                id_clause = f"({page_ids[0]})"
            else:
                id_clause = tuple(page_ids)
            
            # Delete from all related tables in the right order
            # 1. Delete links first (they reference pages)
            cursor.execute(f"""
                DELETE FROM crawldb.link 
                WHERE from_page IN {id_clause}
                OR to_page IN {id_clause}
            """)
            
            # 2. Delete images (they reference pages)
            cursor.execute(f"""
                DELETE FROM crawldb.image
                WHERE page_id IN {id_clause}
            """)
            
            # 3. Delete page_data (they reference pages)
            cursor.execute(f"""
                DELETE FROM crawldb.page_data
                WHERE page_id IN {id_clause}
            """)
            
            # 4. Now it's safe to delete the pages
            cursor.execute(f"DELETE FROM crawldb.page WHERE id IN {id_clause}")
        
        db.conn.commit()
        print("Test data successfully cleaned up")
    except Exception as e:
        print(f"Cleanup error (non-critical): {e}")
    finally:
        cursor.close()

def test_preferential_crawling():
    """Test preferential crawling based on keywords in URLs"""
    print("\n===== PREFERENTIAL CRAWLING TEST =====\n")
    
    db = Database()
    try:
        # Clear existing test data
        clean_test_data(db, [
            "https://example.com/about",
            "https://example.com/research/papers",
            "https://example.com/contact",
            "https://med.over.net/forum/zdravje",
            "https://example.com/news",
            "https://fri.uni-lj.si/sl/raziskave/projects",
            "https://med.over.net/health/article",
            "https://example.com/services"
        ])
        
        # Set up preferential keywords
        keywords = ["research", "forum", "project", "health"]
        print(f"Setting preferential keywords: {keywords}")
        db.set_preferential_keywords(keywords)
        
        # Add test URLs to frontier
        test_urls = [
            "https://example.com/about",
            "https://example.com/research/papers",
            "https://example.com/contact",
            "https://med.over.net/forum/zdravje",
            "https://example.com/news",
            "https://fri.uni-lj.si/sl/raziskave/projects",
            "https://med.over.net/health/article",
            "https://example.com/services"
        ]
        print("Adding test URLs to frontier:")
        for url in test_urls:
            print(f"  - {url}")
            success = db.add_page_to_frontier(url)
            if not success:
                print(f"    ❌ Failed to add {url}")
        
        # IMPORTANT: Verify URLs are actually in the frontier
        found = db.verify_frontier_urls(test_urls)
        if found == 0:
            print("❌ No test URLs were added to the frontier - aborting test")
            return
            
        # Continue with the test...
        
        # Retrieve URLs in preferential order
        print("\nRetrieving URLs in preferential order:")
        retrieved_urls = []
        for i in range(len(test_urls)):
            next_url = db.get_next_frontier_page_preferential()
            if next_url:
                retrieved_urls.append(next_url)
                print(f"  {i+1}. {next_url}")
                db.mark_page_as_processed(next_url)  # Remove from frontier
            else:
                print(f"  {i+1}. No more URLs in frontier")
                break
        
        # Analyze results
        print("\nAnalyzing results:")
        keyword_urls = [url for url in test_urls if any(keyword in url.lower() for keyword in keywords)]
        nonkeyword_urls = [url for url in test_urls if not any(keyword in url.lower() for keyword in keywords)]
        
        print(f"URLs containing keywords: {len(keyword_urls)}")
        print(f"URLs without keywords: {len(nonkeyword_urls)}")
        
        # Check if keyword URLs were prioritized
        keyword_positions = []
        for url in keyword_urls:
            if url in retrieved_urls:
                position = retrieved_urls.index(url) + 1
                keyword_positions.append(position)
                
        if keyword_positions:
            avg_keyword_pos = sum(keyword_positions) / len(keyword_positions)
            avg_expected_pos = (len(keyword_urls) + 1) / 2  # Expected average position if keywords are prioritized
            
            print(f"Average position of keyword URLs: {avg_keyword_pos:.1f}")
            print(f"Expected average position if prioritized: {avg_expected_pos:.1f}")
            
            if avg_keyword_pos <= (len(test_urls) / 2):
                print("✅ Keyword URLs were retrieved with higher priority")
            else:
                print("❌ Keyword prioritization may not be working correctly")
        else:
            print("❌ No keyword URLs were retrieved")
        
        # Test individual keyword behavior
        print("\nTesting individual keywords:")
        
        # Clear frontier again
        cursor = db.conn.cursor()
        cursor.execute("DELETE FROM crawldb.page WHERE page_type_code = 'FRONTIER'")
        db.conn.commit()
        cursor.close()
        
        # Add URL for each keyword
        keyword_specific_urls = {}
        for keyword in keywords:
            url = f"https://example.com/{keyword}-specific-page"
            db.add_page_to_frontier(url)
            keyword_specific_urls[keyword] = url
        
        # Test each keyword
        for keyword in keywords:
            # Set just one keyword
            db.set_preferential_keywords([keyword])
            
            # Get next URL
            next_url = db.get_next_frontier_page_preferential()
            
            # Check if matches expected URL
            if next_url == keyword_specific_urls[keyword]:
                print(f"  ✅ Keyword '{keyword}' - URL correctly prioritized: {next_url}")
            else:
                print(f"  ❌ Keyword '{keyword}' - URL not prioritized. Got: {next_url}")
            
            # Mark as processed to avoid influencing next test
            if next_url:
                db.mark_page_as_processed(next_url)
        
        print("\n===== PREFERENTIAL CRAWLING TEST COMPLETED =====\n")
        
    except Exception as e:
        print(f"Error during preferential crawling testing: {e}")
    # 6. Clean up test data properly
    try:
        cursor = db.conn.cursor()
        # Get all URLs to clean up
        cursor.execute("""
            SELECT url FROM crawldb.page 
            WHERE page_type_code = 'FRONTIER' 
            OR url LIKE 'https://example.com/%'
        """)
        cleanup_urls = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        # Use the helper function to clean up
        clean_test_data(db, cleanup_urls)
    except Exception as e:
        print(f"Cleanup error (non-critical): {e}")

def test_sitemap_extraction_from_robots():
    """Test extraction and processing of sitemaps from robots.txt"""
    try:
        print("\n===== TESTING SITEMAP EXTRACTION FROM ROBOTS.TXT =====\n")
        
        # Create database instance
        db = Database()
        crawler = Crawler(["https://www.sitemaps.org"], max_pages=1)
        
        # Test with the official sitemaps.org domain - perfect for this test
        test_domain = "www.sitemaps.org"
        robots_url = "https://www.sitemaps.org/robots.txt"
        expected_sitemap_url = "https://www.sitemaps.org/sitemap.xml"
        
        print(f"Testing with: {robots_url}")
        
        # 1. Fetch the robots.txt content
        try:
            response = requests.get(robots_url, timeout=10)
            if response.status_code == 200:
                robots_content = response.text
                print(f"✅ Found robots.txt with content length: {len(robots_content)} bytes")
                print(f"Robots.txt content snippet:\n{robots_content[:200]}...")
                
                # Store the robots content
                site_id = db.add_site(test_domain, robots_content)
                print(f"Added site with ID: {site_id}")
                
                # 2. Parse robots.txt to extract sitemap
                base_url = f"https://{test_domain}/"
                robots_parser = crawler.get_robots_parser(base_url)
                
                if hasattr(robots_parser, 'sitemaps') and robots_parser.sitemaps:
                    print(f"✅ Found {len(robots_parser.sitemaps)} sitemaps in robots.txt:")
                    for sitemap_url in robots_parser.sitemaps:
                        print(f"   - {sitemap_url}")
                    
                    # 3. Process the first sitemap
                    crawler.process_sitemap(base_url)
                    
                    # 4. Check if sitemap was stored in database
                    cursor = db.conn.cursor()
                    cursor.execute("SELECT sitemap_content FROM crawldb.site WHERE domain = %s", (test_domain,))
                    result = cursor.fetchone()
                    cursor.close()
                    
                    if result and result[0]:
                        print(f"✅ Sitemap content stored in database, length: {len(result[0])} bytes")
                        print(f"Sitemap content snippet:\n{result[0][:200]}...")
                    else:
                        print("❌ Sitemap content not stored in database")
                else:
                    print("❌ No sitemaps found in robots.txt")
            else:
                print(f"❌ Failed to fetch robots.txt, status code: {response.status_code}")
        except Exception as e:
            print(f"❌ Error fetching robots.txt: {e}")
        
        print("\n===== SITEMAP EXTRACTION TEST COMPLETED =====\n")
        
    except Exception as e:
        print(f"Error during sitemap extraction testing: {e}")

if __name__ == "__main__":
    # Comment out these if you only want to run the real-world test
    test_database_operations()
    test_crawler_extraction()
    test_real_world_extraction()
    test_duplicate_detection()
    test_preferential_crawling()
    test_sitemap_extraction_from_robots()