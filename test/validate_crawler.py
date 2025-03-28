import psycopg2
import argparse
from prettytable import PrettyTable
from datetime import datetime
import dotenv
import os

dotenv.load_dotenv()
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

class CrawlerValidator:
    def __init__(self, host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password):
        """Connect to the database"""
        try:
            self.conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password
            )
            self.conn.autocommit = True
            print("Connected to database successfully!")
        except Exception as e:
            print(f"Database connection error: {e}")
            raise
    
    def run_all_checks(self):
        """Run all validation checks"""
        print("\n===== CRAWLER VALIDATION REPORT =====")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=====================================\n")
        
        self.check_page_counts()
        self.check_html_pages()
        self.check_binary_pages()
        self.check_duplicates()
        self.check_images()
        self.check_links()
        self.check_robots_txt()
        self.check_sitemap()
        self.check_domain_distribution()
        self.check_performance()
        
    def check_page_counts(self):
        """Check if the crawler has processed approximately 5,000 pages"""
        cursor = self.conn.cursor()
        try:
            # Get total pages
            cursor.execute("SELECT COUNT(*) FROM crawldb.page")
            total_pages = cursor.fetchone()[0]
            
            # Get pages by type
            cursor.execute("""
                SELECT page_type_code, COUNT(*) 
                FROM crawldb.page 
                GROUP BY page_type_code
            """)
            page_types = cursor.fetchall()
            
            print(f"1. PAGE COUNT CHECK")
            print(f"Total pages in database: {total_pages}")
            if total_pages < 4500:
                print("❌ FAILED: Less than 4,500 pages crawled (required ~5,000)")
            else:
                print("✅ PASSED: At least 4,500 pages crawled")
            
            # Print page type distribution
            table = PrettyTable()
            table.field_names = ["Page Type", "Count", "Percentage"]
            
            for page_type, count in page_types:
                percentage = (count / total_pages) * 100 if total_pages > 0 else 0
                table.add_row([page_type, count, f"{percentage:.2f}%"])
            
            print("\nPage Type Distribution:")
            print(table)
            print("\n")
            
        finally:
            cursor.close()
    
    def check_html_pages(self):
        """Check HTML page processing"""
        cursor = self.conn.cursor()
        try:
            # Count HTML pages
            cursor.execute("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'HTML'")
            html_count = cursor.fetchone()[0]
            
            # Check for content
            cursor.execute("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'HTML' AND html_content IS NOT NULL")
            with_content = cursor.fetchone()[0]
            
            # Check for content hash
            cursor.execute("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'HTML' AND content_hash IS NOT NULL")
            with_hash = cursor.fetchone()[0]
            
            print(f"2. HTML PAGE CHECK")
            print(f"HTML pages crawled: {html_count}")
            
            # Content checks
            content_percentage = (with_content / html_count) * 100 if html_count > 0 else 0
            print(f"Pages with HTML content: {with_content} ({content_percentage:.2f}%)")
            
            if content_percentage < 90 and html_count > 0:
                print("❌ FAILED: Less than 90% of HTML pages have content")
            else:
                print("✅ PASSED: Most HTML pages have content")
            
            # Hash checks
            hash_percentage = (with_hash / html_count) * 100 if html_count > 0 else 0
            print(f"Pages with content hash: {with_hash} ({hash_percentage:.2f}%)")
            
            if hash_percentage < 90 and html_count > 0:
                print("❌ FAILED: Less than 90% of HTML pages have a content hash for duplicate detection")
            else:
                print("✅ PASSED: Most HTML pages have content hash for duplicate detection")
            
            print("\n")
            
        finally:
            cursor.close()

    def check_binary_pages(self):
        """Check binary content handling"""
        cursor = self.conn.cursor()
        try:
            # Count binary pages
            cursor.execute("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'BINARY'")
            binary_count = cursor.fetchone()[0]
            
            # Count page data entries
            cursor.execute("SELECT COUNT(*) FROM crawldb.page_data")
            page_data_count = cursor.fetchone()[0]
            
            print(f"3. BINARY CONTENT CHECK")
            print(f"Binary pages detected: {binary_count}")
            print(f"Page data entries: {page_data_count}")
            
            if binary_count == 0:
                print("⚠️ WARNING: No binary content detected - crawler might not be detecting binary content")
            else:
                print("✅ PASSED: Binary content detection working")
                
            # Get binary content types
            cursor.execute("""
                SELECT data_type_code, COUNT(*) 
                FROM crawldb.page_data 
                GROUP BY data_type_code 
                ORDER BY COUNT(*) DESC
                LIMIT 5
            """)
            types = cursor.fetchall()
            
            if types:
                print("\nTop binary content types:")
                table = PrettyTable()
                table.field_names = ["Type", "Count"]
                
                for data_type, count in types:
                    table.add_row([data_type, count])
                
                print(table)
            
            print("\n")
            
        finally:
            cursor.close()
    
    def check_duplicates(self):
        """Check duplicate page detection"""
        cursor = self.conn.cursor()
        try:
            # Count duplicate pages
            cursor.execute("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'DUPLICATE'")
            duplicate_count = cursor.fetchone()[0]
            
            print(f"4. DUPLICATE PAGE CHECK")
            print(f"Duplicate pages detected: {duplicate_count}")
            
            if duplicate_count == 0:
                print("⚠️ WARNING: No duplicate pages detected - duplicate detection might not be working")
            else:
                print("✅ PASSED: Duplicate detection working")
            
            print("\n")
            
        finally:
            cursor.close()
            
    def check_images(self):
        """Check image extraction"""
        cursor = self.conn.cursor()
        try:
            # Count images
            cursor.execute("SELECT COUNT(*) FROM crawldb.image")
            image_count = cursor.fetchone()[0]
            
            print(f"5. IMAGE EXTRACTION CHECK")
            print(f"Images extracted: {image_count}")
            
            if image_count == 0:
                print("❌ FAILED: No images extracted")
            else:
                print("✅ PASSED: Images are being extracted")
            
            # Get image type distribution
            cursor.execute("""
                SELECT 
                    SUBSTRING(content_type FROM POSITION('/' IN content_type) + 1) as extension,
                    COUNT(*) 
                FROM crawldb.image 
                GROUP BY extension 
                ORDER BY COUNT(*) DESC
                LIMIT 5
            """)
            types = cursor.fetchall()
            
            if types:
                print("\nTop image types:")
                table = PrettyTable()
                table.field_names = ["Type", "Count"]
                
                for img_type, count in types:
                    table.add_row([img_type, count])
                
                print(table)
            
            print("\n")
            
        finally:
            cursor.close()

    def check_links(self):
        """Check link extraction"""
        cursor = self.conn.cursor()
        try:
            # Count links
            cursor.execute("SELECT COUNT(*) FROM crawldb.link")
            link_count = cursor.fetchone()[0]
            
            # Count pages with outgoing links
            cursor.execute("SELECT COUNT(DISTINCT from_page) FROM crawldb.link")
            pages_with_links = cursor.fetchone()[0]
            
            print(f"6. LINK EXTRACTION CHECK")
            print(f"Links extracted: {link_count}")
            print(f"Pages with outgoing links: {pages_with_links}")
            
            if link_count == 0:
                print("❌ FAILED: No links extracted")
            else:
                print("✅ PASSED: Links are being extracted")
                
                # Calculate average links per page
                if pages_with_links > 0:
                    avg_links = link_count / pages_with_links
                    print(f"Average links per page: {avg_links:.2f}")
            
            print("\n")
            
        finally:
            cursor.close()
    
    def check_robots_txt(self):
        """Check robots.txt processing"""
        cursor = self.conn.cursor()
        try:
            # Count sites with robots.txt
            cursor.execute("SELECT COUNT(*) FROM crawldb.site WHERE robots_content IS NOT NULL")
            with_robots = cursor.fetchone()[0]
            
            # Count total sites
            cursor.execute("SELECT COUNT(*) FROM crawldb.site")
            total_sites = cursor.fetchone()[0]
            
            print(f"7. ROBOTS.TXT CHECK")
            print(f"Total sites: {total_sites}")
            print(f"Sites with robots.txt: {with_robots}")
            
            robots_percentage = (with_robots / total_sites) * 100 if total_sites > 0 else 0
            print(f"Percentage: {robots_percentage:.2f}%")
            
            if with_robots == 0:
                print("❌ FAILED: No robots.txt files processed")
            else:
                print("✅ PASSED: robots.txt files are being processed")
            
            print("\n")
            
        finally:
            cursor.close()
    
    def check_sitemap(self):
        """Check sitemap processing"""
        cursor = self.conn.cursor()
        try:
            # Count sites with sitemap
            cursor.execute("SELECT COUNT(*) FROM crawldb.site WHERE sitemap_content IS NOT NULL")
            with_sitemap = cursor.fetchone()[0]
            
            # Count total sites
            cursor.execute("SELECT COUNT(*) FROM crawldb.site")
            total_sites = cursor.fetchone()[0]
            
            print(f"8. SITEMAP CHECK")
            print(f"Total sites: {total_sites}")
            print(f"Sites with sitemap: {with_sitemap}")
            
            sitemap_percentage = (with_sitemap / total_sites) * 100 if total_sites > 0 else 0
            print(f"Percentage: {sitemap_percentage:.2f}%")
            
            # This is not a strict requirement, so just warn
            if with_sitemap == 0:
                print("⚠️ WARNING: No sitemaps processed")
            else:
                print("✅ PASSED: Sitemaps are being processed")
            
            print("\n")
            
        finally:
            cursor.close()
    
    def check_domain_distribution(self):
        """Check distribution of med.over.net pages"""
        cursor = self.conn.cursor()
        try:
            # Get section distribution
            cursor.execute("""
                SELECT 
                    CASE
                        WHEN url LIKE '%/forum/zdravje/%' THEN '/forum/zdravje'
                        WHEN url LIKE '%/forum/nosecnost-in-otroci/%' THEN '/forum/nosecnost-in-otroci'
                        WHEN url LIKE '%/forum/dusevno-zdravje/%' THEN '/forum/dusevno-zdravje'
                        WHEN url LIKE '%/forum/%' THEN '/forum (other)'
                        ELSE 'other'
                    END AS section,
                    COUNT(*) 
                FROM crawldb.page
                WHERE url LIKE 'https://med.over.net%'
                AND page_type_code = 'HTML'
                GROUP BY section
                ORDER BY COUNT(*) DESC
            """)
            sections = cursor.fetchall()
            
            print(f"9. DOMAIN DISTRIBUTION CHECK")
            
            if not sections:
                print("❌ FAILED: No med.over.net pages found")
            else:
                print("✅ PASSED: Pages from med.over.net found")
                
                print("\nSection distribution:")
                table = PrettyTable()
                table.field_names = ["Section", "Count"]
                
                for section, count in sections:
                    table.add_row([section, count])
                
                print(table)
            
            print("\n")
            
        finally:
            cursor.close()
    
    def check_performance(self):
        """Check crawler performance metrics"""
        cursor = self.conn.cursor()
        try:
            # Get crawling time span
            cursor.execute("""
                SELECT 
                    MIN(accessed_time) as first_access,
                    MAX(accessed_time) as last_access
                FROM crawldb.page
                WHERE accessed_time IS NOT NULL
            """)
            result = cursor.fetchone()
            
            print(f"10. PERFORMANCE METRICS")
            
            if not result[0] or not result[1]:
                print("⚠️ WARNING: No timestamp data available")
                return
                
            first_access, last_access = result
            duration = last_access - first_access
            
            print(f"First page crawled: {first_access}")
            print(f"Last page crawled: {last_access}")
            print(f"Total crawling duration: {duration}")
            
            # Calculate crawling rate
            cursor.execute("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'HTML'")
            html_count = cursor.fetchone()[0]
            
            duration_seconds = duration.total_seconds()
            if duration_seconds > 0:
                pages_per_second = html_count / duration_seconds
                pages_per_hour = pages_per_second * 3600
                print(f"Average crawling rate: {pages_per_hour:.2f} pages/hour")
                
                # Check if crawling rate is reasonable
                if pages_per_hour < 10:
                    print("⚠️ WARNING: Crawling rate is very low")
                elif pages_per_hour > 10000:
                    print("⚠️ WARNING: Crawling rate is suspiciously high")
            
            print("\n")
            
        finally:
            cursor.close()

def main():
    parser = argparse.ArgumentParser(description='Validate web crawler data extraction')
    
    # Use environment variables as defaults, fallback to hardcoded values if env vars not set
    parser.add_argument('--host', default=db_host or 'localhost', 
                       help='Database host (default: from .env or localhost)')
    parser.add_argument('--port', type=int, default=int(db_port or 5432), 
                       help='Database port (default: from .env or 5432)')
    parser.add_argument('--db', default=db_name or 'wier', 
                       help='Database name (default: from .env or wier)')
    parser.add_argument('--user', default=db_user or 'user', 
                       help='Database user (default: from .env or user)')
    parser.add_argument('--password', default=db_password or 'SecretPassword', 
                       help='Database password (default: from .env or SecretPassword)')
    
    args = parser.parse_args()
    
    try:
        validator = CrawlerValidator(
            host=args.host,
            port=args.port,
            dbname=args.db,
            user=args.user,
            password=args.password
        )
        validator.run_all_checks()
    except Exception as e:
        print(f"Error during validation: {e}")

if __name__ == "__main__":
    main()