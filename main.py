import concurrent.futures
import threading
from crawler import Crawler
import argparse
import time
from datetime import datetime
from database import Database
from shared_queue import SharedURLQueue

def main(num_workers=4, max_pages_per_worker=1250, debug_mode=False):
    # Calculate total target pages
    total_pages = num_workers * max_pages_per_worker
    
    # Initialize seed URLs
    seed_urls = [
       "https://www.fri.uni-lj.si/",
       "https://fri.uni-lj.si/sl/studij/",
       "https://fri.uni-lj.si/sl/raziskave/",
       "https://fri.uni-lj.si/sl/raziskave/projekti/",
    ]
    
    # Define preferential keywords
    crawl_keywords = [
        "raziskave", "research", 
        "projekti", "projects",
        "erasmus"
    ]
    
    print(f"Starting crawler with {num_workers} workers")
    print(f"Each worker will crawl up to {max_pages_per_worker} pages")
    print(f"Preferential keywords: {crawl_keywords}")
    print(f"Total target: {total_pages} pages")
    print(f"Starting time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create a single database connection
    db = Database()
    
    # Set up shared URL queue
    url_queue = SharedURLQueue()
    
    # Check for existing frontier URLs and populate queue
    frontier_count = url_queue.populate_from_database(db, limit=5000)
    if frontier_count > 0:
        print(f"Loaded {frontier_count} existing frontier URLs from database to continue crawling")
    
    # Add seed URLs only if we didn't find any frontier pages
    if frontier_count == 0:
        print("No existing frontier URLs found - adding seed URLs")
        for url in seed_urls:
            db.add_page_to_frontier(url, priority=0)
            url_queue.add_url(url, priority=0)
    
    # Set preferential keywords
    db.set_preferential_keywords(crawl_keywords)
    url_queue.set_preferential_keywords(crawl_keywords)  # Add this line
    
    start_time = time.time()
    
    # Create and start worker threads
    threads = []
    for i in range(num_workers):
        thread = threading.Thread(
            target=worker_thread_function,
            args=(db, url_queue, max_pages_per_worker, i)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to finish
    for thread in threads:
        thread.join()
    
    # Calculate total time
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"Crawling complete! Targeted approximately {total_pages} pages.")
    print(f"Total execution time: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("Performing final duplicate detection...")
    crawler = Crawler([])
    crawler.db = db
    crawler.find_duplicates()

def worker_thread_function(db, url_queue, max_pages, worker_id):
    """Worker function that uses shared database and URL queue"""
    try:
        # Create crawler for this worker
        crawler = Crawler([], max_pages=max_pages)
        crawler.db = db  # Use the shared database
        crawler.url_queue = url_queue  # Give crawler access to shared queue
    
        pages_crawled = 0  # Initialize pages_crawled
        empty_queue_count = 0
        
        while pages_crawled < max_pages:
            # Get next URL from shared queue
            next_url = url_queue.get_next_url()
            
            if not next_url:
                # If queue is empty, try to get URLs from database
                db_url = db.get_next_frontier_page_preferential()
                if db_url:
                    next_url = db_url
                    url_queue.add_url(next_url)  # Add back to queue
                else:
                    empty_queue_count += 1
                    print(f"Worker {worker_id}: No URLs in queue! Attempt {empty_queue_count}/5")
                    if empty_queue_count >= 5:
                        print(f"Worker {worker_id}: No more URLs after 5 attempts! Exiting.")
                        break
                    time.sleep(5)
                    continue
            
            empty_queue_count = 0
            
            print(f"Worker {worker_id} crawling ({pages_crawled+1}/{max_pages}): {next_url}")
            success = crawler.crawl_page(next_url)
            
            # Mark as processed regardless of success
            url_queue.mark_url_processed(next_url)
            
            # If successful, extract links and add to queue
            if success:
                pages_crawled += 1
                # Only mark as HTML if successful
                db.mark_page_as_processed(next_url, 'HTML')
            else:
                # Mark as FRONTIER again to retry later
                db.add_page_to_frontier(next_url)
            
            # Be polite
            time.sleep(3)
            
        print(f"Worker {worker_id} finished after crawling {pages_crawled} pages")
        return f"Worker {worker_id} completed successfully"
    except Exception as e:
        print(f"Worker {worker_id} error: {e}")
        return f"Worker {worker_id} failed: {e}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run web crawler with multiple workers')
    parser.add_argument('--workers', type=int, default=10, help='Number of worker processes')
    parser.add_argument('--max-pages', type=int, default=500, help='Max pages per worker')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode with fewer pages')
    args = parser.parse_args()
    
    if args.debug:
        print("Running in debug mode with a single worker and 5 pages")
        run_worker(["https://med.over.net/"], 5, 0)
    else:
        main(num_workers=args.workers, max_pages_per_worker=args.max_pages)