import heapq
import threading

class SharedURLQueue:
    """Thread-safe shared URL queue with priority support"""
    
    def __init__(self):
        self.queue = []  # For heapq operations
        self.visited = set()
        self.currently_processing = set()
        self.queue_lock = threading.Lock()
        self.preferential_keywords = []
    
    
    def get_next_url(self):
        """Get next URL from queue with thread safety"""
        with self.queue_lock:
            if not self.queue:
                return None
                
            _, url = heapq.heappop(self.queue)
            self.currently_processing.add(url)
            return url
    
    def mark_url_processed(self, url):
        """Mark URL as processed, removing from processing set and adding to visited"""
        with self.queue_lock:
            if url in self.currently_processing:
                self.currently_processing.remove(url)
            self.visited.add(url)
    
    def get_queue_size(self):
        """Get current queue size"""
        with self.queue_lock:
            return len(self.queue)
    
    def populate_from_database(self, db, limit=1000):
        """Populate queue with frontier URLs from database"""
        with self.queue_lock:
            # Get batch of frontier URLs from database
            frontier_urls = db.get_frontier_batch(limit)
            count = 0
            
            # Add URLs to queue if not already visited
            for url, priority in frontier_urls:
                if url not in self.visited and url not in self.currently_processing:
                    heapq.heappush(self.queue, (priority or 0, url))
                    count += 1
                    
            return count
    
    def add_url(self, url, priority=0):
        """Add URL to queue with priority (lower number = higher priority)"""
        with self.queue_lock:
            if url in self.visited or url in self.currently_processing:
                return False
                
            # Adjust priority if URL contains preferential keywords
            if self.preferential_keywords:
                for keyword in self.preferential_keywords:
                    if keyword.lower() in url.lower():
                        priority = max(0, priority - 1)  # Lower number = higher priority
                        break
            
            heapq.heappush(self.queue, (priority, url))
            return True
    
    def set_preferential_keywords(self, keywords):
        """Set keywords for preferential URL selection"""
        self.preferential_keywords = keywords