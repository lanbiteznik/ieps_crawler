import psycopg2
import argparse
from datetime import datetime, timedelta
import time
import sys
from prettytable import PrettyTable
import matplotlib.pyplot as plt

class CrawlerMonitor:
    def __init__(self):
        try:
            # Connect to database with the same credentials as your crawler
            self.conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="wier",
                user="postgres",
                password="admin"
            )
            self.conn.autocommit = True
        except Exception as e:
            print(f"Database connection error: {e}")
            raise

    def get_crawling_stats(self):
        """Get overall statistics about the crawled pages"""
        cursor = self.conn.cursor()
        try:
            # Get page counts by type
            cursor.execute("""
                SELECT 
                    page_type_code, 
                    COUNT(*) 
                FROM crawldb.page 
                GROUP BY page_type_code
            """)
            page_types = dict(cursor.fetchall())
            
            # Get domain counts
            cursor.execute("""
                SELECT 
                    s.domain,
                    COUNT(p.id) AS page_count
                FROM crawldb.site s
                JOIN crawldb.page p ON p.site_id = s.id
                WHERE p.page_type_code = 'HTML'
                GROUP BY s.domain
                ORDER BY page_count DESC
                LIMIT 10
            """)
            domains = cursor.fetchall()
            
            # Get total number of images
            cursor.execute("SELECT COUNT(*) FROM crawldb.image")
            image_count = cursor.fetchone()[0]
            
            # Get total number of links
            cursor.execute("SELECT COUNT(*) FROM crawldb.link")
            link_count = cursor.fetchone()[0]
            
            # Get crawling rate
            cursor.execute("""
                SELECT 
                    MIN(accessed_time) AS first_access,
                    MAX(accessed_time) AS last_access,
                    COUNT(*) AS page_count
                FROM crawldb.page
                WHERE page_type_code = 'HTML' AND accessed_time IS NOT NULL
            """)
            result = cursor.fetchone()
            if result[0] and result[1]:
                first_access, last_access, page_count = result
                duration = (last_access - first_access).total_seconds()
                crawl_rate = page_count / duration * 3600 if duration > 0 else 0
            else:
                first_access, last_access, crawl_rate = None, None, 0
                
            return {
                'page_types': page_types,
                'domains': domains,
                'image_count': image_count,
                'link_count': link_count,
                'first_access': first_access,
                'last_access': last_access,
                'crawl_rate': crawl_rate
            }
        finally:
            cursor.close()
    
    def get_med_over_net_sections(self):
        """Get stats about different med.over.net forum sections"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    CASE
                        WHEN url LIKE '%/forum/zdravje/%' THEN 'zdravje'
                        WHEN url LIKE '%/forum/nosecnost-in-otroci/%' THEN 'nosecnost'
                        WHEN url LIKE '%/forum/dusevno-zdravje/%' THEN 'dusevno-zdravje'
                        WHEN url LIKE '%/forum/%' THEN 'other_forums'
                        ELSE 'non_forum'
                    END AS section,
                    COUNT(*) AS count
                FROM crawldb.page
                WHERE page_type_code = 'HTML'
                GROUP BY section
                ORDER BY count DESC
            """)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def display_stats(self):
        """Display crawling statistics in a nice format"""
        stats = self.get_crawling_stats()
        
        print("\n===== CRAWLER STATISTICS =====")
        
        # Display page type counts
        types_table = PrettyTable()
        types_table.field_names = ["Page Type", "Count"]
        for page_type, count in stats['page_types'].items():
            types_table.add_row([page_type, count])
        print("\nPage Types:")
        print(types_table)
        
        # Display domain counts
        domains_table = PrettyTable()
        domains_table.field_names = ["Domain", "Page Count"]
        for domain, count in stats['domains']:
            domains_table.add_row([domain, count])
        print("\nTop Domains:")
        print(domains_table)
        
        # Display image and link counts
        print(f"\nTotal Images: {stats['image_count']}")
        print(f"Total Links: {stats['link_count']}")
        
        # Display crawl rate
        if stats['first_access'] and stats['last_access']:
            print(f"\nCrawling started: {stats['first_access']}")
            print(f"Last crawled page: {stats['last_access']}")
            duration = stats['last_access'] - stats['first_access']
            print(f"Crawling duration: {duration}")
            print(f"Average crawl rate: {stats['crawl_rate']:.2f} pages per hour")
        
        # Display med.over.net section stats
        sections = self.get_med_over_net_sections()
        sections_table = PrettyTable()
        sections_table.field_names = ["Section", "Page Count"]
        for section, count in sections:
            sections_table.add_row([section, count])
        print("\nMed.over.net Sections:")
        print(sections_table)
    
    def plot_page_types(self):
        """Plot a pie chart of page types"""
        stats = self.get_crawling_stats()
        page_types = stats['page_types']
        
        # Create a pie chart
        labels = list(page_types.keys())
        sizes = list(page_types.values())
        
        plt.figure(figsize=(10, 6))
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
        plt.title('Distribution of Page Types')
        plt.savefig('page_types.png')
        print("\nCreated page_types.png chart")
    
    def monitor_crawl(self, interval=10):
        """Monitor crawling progress in real-time"""
        print("\n===== CRAWLER MONITORING =====")
        print("Press Ctrl+C to stop monitoring\n")
        
        try:
            while True:
                cursor = self.conn.cursor()
                
                # Get current counts
                cursor.execute("""
                    SELECT page_type_code, COUNT(*) FROM crawldb.page GROUP BY page_type_code
                """)
                page_types = dict(cursor.fetchall())
                
                # Get recent crawl rate
                cursor.execute("""
                    SELECT COUNT(*) FROM crawldb.page 
                    WHERE page_type_code = 'HTML' 
                    AND accessed_time > %s
                """, (datetime.now() - timedelta(minutes=1),))
                recent_rate = cursor.fetchone()[0]
                
                # Clear screen
                sys.stdout.write("\033[H\033[J")
                
                # Print status
                print(f"=== Crawler Status (Updated: {datetime.now().strftime('%H:%M:%S')}) ===")
                print(f"HTML Pages: {page_types.get('HTML', 0)}")
                print(f"Frontier Pages: {page_types.get('FRONTIER', 0)}")
                print(f"Binary Pages: {page_types.get('BINARY', 0)}")
                print(f"Duplicate Pages: {page_types.get('DUPLICATE', 0)}")
                print(f"Current Rate: {recent_rate} pages/minute")
                print(f"\nEstimated completion: {estimate_completion(page_types.get('HTML', 0), recent_rate, 5000)}")
                
                cursor.close()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped")

def estimate_completion(current_pages, rate_per_minute, total_pages):
    """Estimate completion time based on current progress and rate"""
    if rate_per_minute <= 0:
        return "Unknown (no recent activity)"
    
    remaining_pages = total_pages - current_pages
    if remaining_pages <= 0:
        return "Complete!"
    
    minutes_remaining = remaining_pages / rate_per_minute
    if minutes_remaining < 60:
        return f"~{int(minutes_remaining)} minutes"
    else:
        hours_remaining = minutes_remaining / 60
        return f"~{int(hours_remaining)} hours, {int(minutes_remaining % 60)} minutes"

def main():
    parser = argparse.ArgumentParser(description='Monitor med.over.net crawler')
    parser.add_argument('--mode', choices=['stats', 'monitor', 'plot'], default='stats', 
                        help='Mode: stats (show statistics), monitor (real-time monitoring), plot (create charts)')
    parser.add_argument('--interval', type=int, default=10, help='Monitoring refresh interval in seconds')
    args = parser.parse_args()
    
    monitor = CrawlerMonitor()
    
    if args.mode == 'stats':
        monitor.display_stats()
    elif args.mode == 'monitor':
        monitor.monitor_crawl(args.interval)
    elif args.mode == 'plot':
        monitor.plot_page_types()

if __name__ == "__main__":
    main()