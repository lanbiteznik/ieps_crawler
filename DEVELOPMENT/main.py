import concurrent.futures
from DEVELOPMENT.crawler import Crawler
import argparse
import time
from datetime import datetime
from DEVELOPMENT.database import Database

def run_worker(seed_urls, max_pages, worker_id, keywords=None):
    try:
        print(f"Starting worker {worker_id} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Worker {worker_id} will crawl up to {max_pages} pages")
        
        # Initialize database for this worker
        db = Database()
        if keywords:
            db.set_preferential_keywords(keywords)
        
        # Initialize crawler
        crawler = Crawler(seed_urls, max_pages)
        crawler.start()
        
        return f"Worker {worker_id} completed successfully"
    except Exception as e:
        print(f"Worker {worker_id} encountered an error: {e}")
        return f"Worker {worker_id} failed: {str(e)}"
    
def main(num_workers=4, max_pages_per_worker=1250, debug_mode=False):
    # Calculate total target pages
    total_pages = num_workers * max_pages_per_worker
    
    # The med.over.net site with forum sections that will provide good crawling depth
    seed_urls = [
       "https://www.fri.uni-lj.si/",
       "https://fri.uni-lj.si/sl/studij/",
       "https://fri.uni-lj.si/sl/raziskave/",
       "https://fri.uni-lj.si/sl/raziskave/projekti/",
       #"https://med.over.net/",
       #"https://med.over.net/forum/",
       #"https://med.over.net/forum/zdravje/"
    ]
    
    # Define preferential crawling keywords
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
    from DEVELOPMENT.database import Database
    db = Database()
    invalid_count = db.cleanup_invalid_sites()
    if invalid_count > 0:
        print(f"WARNING: Found {invalid_count} invalid domains in the site table.")
        print("These will not be accessible via web browsers.")
    
    # Set preferential keywords
    db.set_preferential_keywords(crawl_keywords)
    
    start_time = time.time()
    
    # Create a list to hold the futures
    futures = []
    
    # Distribute seed URLs among workers
    worker_seeds = []
    for i in range(num_workers):
        # Each worker gets one seed URL, cycling through the available URLs
        worker_idx = i % len(seed_urls)
        worker_seeds.append([seed_urls[worker_idx]])
    
    # Use ThreadPoolExecutor as context manager
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit tasks and collect futures
        for i in range(num_workers):
            future = executor.submit(
                run_worker, 
                worker_seeds[i], 
                max_pages_per_worker, 
                i, 
                crawl_keywords
            )
            futures.append(future)
        
        # Wait for all futures to complete
        # This is important - use as_completed to process results as they finish
        for future in concurrent.futures.as_completed(futures):
            try:
                # Get the result - this will propagate any exceptions from the worker
                result = future.result()
                print(f"Worker completed with result: {result}")
            except Exception as exc:
                print(f"Worker generated an exception: {exc}")
    
    # All workers have completed at this point
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"Crawling complete! Targeted approximately {total_pages} pages.")
    print(f"Total execution time: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run web crawler with multiple workers')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker processes')
    parser.add_argument('--max-pages', type=int, default=1250, help='Max pages per worker')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode with fewer pages')
    args = parser.parse_args()
    
    if args.debug:
        print("Running in debug mode with a single worker and 5 pages")
        run_worker(["https://med.over.net/"], 5, 0)
    else:
        main(num_workers=args.workers, max_pages_per_worker=args.max_pages)