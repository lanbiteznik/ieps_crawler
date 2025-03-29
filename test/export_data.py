import os
import sys
import psycopg2
import hashlib
from datetime import datetime
import json
from urllib.parse import urlparse

# Add the parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DEVELOPMENT.database import Database

def extract_content_samples():
    """Extract one example of each content type from the database"""
    print("=== Extracting Content Type Samples ===")
    print(f"Execution time: {datetime.now()}")
    
    # Create output directory
    output_dir = "content_samples"
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to database
    db = Database()
    
    # Extract one example of each content type
    extract_html_example(db, output_dir)
    extract_binary_examples(db, output_dir)
    extract_image_examples(db, output_dir)
    extract_duplicate_example(db, output_dir)
    extract_site_example(db, output_dir)
    extract_link_example(db, output_dir)
    
    print("\n=== Content Extraction Complete ===")

def extract_html_example(db, output_dir):
    """Extract one example of HTML content"""
    print("\n1. Extracting HTML example...")
    cursor = db.conn.cursor()
    
    try:
        # Get one HTML page (preferably from med.over.net and not too large)
        cursor.execute("""
            SELECT id, url, html_content, http_status_code, accessed_time
            FROM crawldb.page 
            WHERE page_type_code = 'HTML' 
            AND html_content IS NOT NULL 
            AND LENGTH(html_content) > 1000
            AND LENGTH(html_content) < 100000
            AND url LIKE 'https://med.over.net%'
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        if not result:
            # Try any HTML page if med.over.net isn't available
            cursor.execute("""
                SELECT id, url, html_content, http_status_code, accessed_time
                FROM crawldb.page 
                WHERE page_type_code = 'HTML' 
                AND html_content IS NOT NULL 
                LIMIT 1
            """)
            result = cursor.fetchone()
        
        if result:
            page_id, url, html_content, status, accessed = result
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            
            # Save HTML file
            html_file = f"{output_dir}/page_{page_id}_{url_hash}.html"
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # Save metadata
            meta_file = f"{output_dir}/page_{page_id}_{url_hash}_meta.json"
            metadata = {
                "page_id": page_id,
                "url": url,
                "status_code": status,
                "accessed_time": str(accessed),
                "content_size_bytes": len(html_content)
            }
            
            with open(meta_file, 'w') as f:
                json.dump(metadata, f, indent=2)
                
            print(f"✅ HTML page saved: {html_file}")
            print(f"   URL: {url}")
            print(f"   Size: {len(html_content)/1024:.1f} KB")
        else:
            print("❌ No HTML pages found in database")
    except Exception as e:
        print(f"Error extracting HTML example: {e}")
    finally:
        cursor.close()

def extract_binary_examples(db, output_dir):
    """Extract one example of each binary content type"""
    print("\n2. Extracting binary examples...")
    cursor = db.conn.cursor()
    
    try:
        # Get distinct binary content types in the database
        cursor.execute("""
            SELECT DISTINCT data_type_code
            FROM crawldb.page_data
        """)
        
        data_types = [row[0] for row in cursor.fetchall()]
        
        if not data_types:
            print("❌ No binary data types found in database")
            return
            
        print(f"   Found {len(data_types)} binary data types: {', '.join(data_types)}")
        
        # Extract one example of each type
        for data_type in data_types:
            cursor.execute("""
                SELECT p.id, p.url, pd.data, pd.data_type_code
                FROM crawldb.page_data pd
                JOIN crawldb.page p ON pd.page_id = p.id
                WHERE pd.data_type_code = %s
                LIMIT 1
            """, (data_type,))
            
            result = cursor.fetchone()
            
            if result:
                page_id, url, binary_data, data_type_code = result
                url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
                extension = data_type_code.lower()
                filename = f"{output_dir}/binary_{page_id}_{url_hash}.{extension}"
                
                # Save binary file
                with open(filename, 'wb') as f:
                    f.write(binary_data)
                
                size_kb = len(binary_data) / 1024
                print(f"✅ {data_type} sample saved: {filename}")
                print(f"   URL: {url}")
                print(f"   Size: {size_kb:.1f} KB")
            else:
                print(f"❌ No {data_type} data found")
                
    except Exception as e:
        print(f"Error extracting binary examples: {e}")
    finally:
        cursor.close()

def extract_image_examples(db, output_dir):
    """Extract one example of each image type"""
    print("\n3. Extracting image examples...")
    cursor = db.conn.cursor()
    
    try:
        # Get distinct image content types
        cursor.execute("""
            SELECT DISTINCT 
                SUBSTRING(content_type FROM POSITION('/' IN content_type) + 1) as extension
            FROM crawldb.image
            WHERE content_type LIKE '%/%'
        """)
        
        extensions = [row[0] for row in cursor.fetchall()]
        
        if not extensions:
            print("❌ No image types found in database")
            return
            
        print(f"   Found {len(extensions)} image types: {', '.join(extensions)}")
        
        # Extract one example of each type
        for ext in extensions:
            cursor.execute("""
                SELECT id, page_id, filename, content_type, data
                FROM crawldb.image
                WHERE content_type LIKE %s AND data IS NOT NULL
                LIMIT 1
            """, (f'%/{ext}',))
            
            result = cursor.fetchone()
            
            if result:
                img_id, page_id, filename, content_type, img_data = result
                
                if img_data:
                    # Sanitize filename
                    safe_filename = "".join(c if c.isalnum() or c in ".-_" else "_" for c in filename)
                    output_file = f"{output_dir}/image_{img_id}_{safe_filename}"
                    
                    # Save image file
                    with open(output_file, 'wb') as f:
                        f.write(img_data)
                    
                    size_kb = len(img_data) / 1024
                    print(f"✅ {ext} image saved: {output_file}")
                    print(f"   Original filename: {filename}")
                    print(f"   Content-Type: {content_type}")
                    print(f"   Size: {size_kb:.1f} KB")
                else:
                    print(f"❌ {ext} image found but has no data")
            else:
                # Try with data IS NULL (metadata only)
                cursor.execute("""
                    SELECT id, page_id, filename, content_type
                    FROM crawldb.image
                    WHERE content_type LIKE %s
                    LIMIT 1
                """, (f'%/{ext}',))
                
                result = cursor.fetchone()
                if result:
                    img_id, page_id, filename, content_type = result
                    print(f"ℹ️ {ext} image exists but only as metadata: {filename}")
                else:
                    print(f"❌ No {ext} images found")
                
    except Exception as e:
        print(f"Error extracting image examples: {e}")
    finally:
        cursor.close()

def extract_duplicate_example(db, output_dir):
    """Extract an example of a duplicate page"""
    print("\n4. Extracting duplicate page example...")
    cursor = db.conn.cursor()
    
    try:
        # Find a duplicate page
        cursor.execute("""
            SELECT p1.id, p1.url, p2.url as original_url
            FROM crawldb.page p1
            JOIN crawldb.page p2 ON p1.id = p2.id
            WHERE p1.page_type_code = 'DUPLICATE'
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        
        if result:
            dup_id, dup_url, original_url = result
            
            # Save metadata
            meta_file = f"{output_dir}/duplicate_{dup_id}_info.json"
            metadata = {
                "duplicate_id": dup_id,
                "duplicate_url": dup_url,
                "original_url": original_url
            }
            
            with open(meta_file, 'w') as f:
                json.dump(metadata, f, indent=2)
                
            print(f"✅ Duplicate page info saved: {meta_file}")
            print(f"   Duplicate URL: {dup_url}")
            print(f"   Original URL: {original_url}")
        else:
            print("❌ No duplicate pages found in database")
                
    except Exception as e:
        print(f"Error extracting duplicate example: {e}")
    finally:
        cursor.close()

def extract_site_example(db, output_dir):
    """Extract an example of site information"""
    print("\n5. Extracting site information example...")
    cursor = db.conn.cursor()
    
    try:
        # Find a site with robots.txt content
        cursor.execute("""
            SELECT id, domain, robots_content, sitemap_content
            FROM crawldb.site
            WHERE robots_content IS NOT NULL
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        
        if result:
            site_id, domain, robots, sitemap = result
            
            # Save robots.txt
            robots_file = f"{output_dir}/site_{site_id}_{domain}_robots.txt"
            if robots:
                with open(robots_file, 'w', encoding='utf-8') as f:
                    f.write(robots)
                print(f"✅ Robots.txt saved: {robots_file}")
            
            # Save sitemap if exists
            if sitemap:
                sitemap_file = f"{output_dir}/site_{site_id}_{domain}_sitemap.xml"
                with open(sitemap_file, 'w', encoding='utf-8') as f:
                    f.write(sitemap)
                print(f"✅ Sitemap saved: {sitemap_file}")
            else:
                print("ℹ️ No sitemap content available for this site")
                
            print(f"   Domain: {domain}")
        else:
            print("❌ No sites with robots.txt found in database")
                
    except Exception as e:
        print(f"Error extracting site example: {e}")
    finally:
        cursor.close()

def extract_link_example(db, output_dir):
    """Extract an example of link structure"""
    print("\n6. Extracting link example...")
    cursor = db.conn.cursor()
    
    try:
        # Find a page with multiple outgoing links
        cursor.execute("""
            SELECT from_page, COUNT(*) as link_count
            FROM crawldb.link
            GROUP BY from_page
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        
        if result:
            from_page_id, link_count = result
            
            # Get the source page URL
            cursor.execute("SELECT url FROM crawldb.page WHERE id = %s", (from_page_id,))
            from_url = cursor.fetchone()[0]
            
            # Get linked pages (limit to 20 to avoid large files)
            cursor.execute("""
                SELECT p.url
                FROM crawldb.link l
                JOIN crawldb.page p ON l.to_page = p.id
                WHERE l.from_page = %s
                LIMIT 20
            """, (from_page_id,))
            
            to_urls = [row[0] for row in cursor.fetchall()]
            
            # Save link information
            link_file = f"{output_dir}/links_page_{from_page_id}_info.json"
            link_data = {
                "source_page_id": from_page_id,
                "source_url": from_url,
                "total_links": link_count,
                "sample_links": to_urls
            }
            
            with open(link_file, 'w') as f:
                json.dump(link_data, f, indent=2)
                
            print(f"✅ Link information saved: {link_file}")
            print(f"   Source URL: {from_url}")
            print(f"   Total outgoing links: {link_count}")
            print(f"   Sample size: {len(to_urls)} links")
        else:
            print("❌ No links found in database")
                
    except Exception as e:
        print(f"Error extracting link example: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    extract_content_samples()