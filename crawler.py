from datetime import datetime
import re
import requests
from urllib.parse import urlparse, urljoin, urlunparse
from database import Database
import time
import hashlib
import robotexclusionrulesparser
import os
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from datasketch import MinHash
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
import base64
#todo scroll 5000 html pages. 

class CustomRobotsParser:
    """Wrapper for robotexclusionrulesparser with additional attributes"""
    def __init__(self):
        self.parser = robotexclusionrulesparser.RobotExclusionRulesParser()
        self.sitemaps = []
        self.crawl_delay = 5
    
    def parse(self, content):
        """Parse robots.txt content"""
        return self.parser.parse(content)
    
    def is_allowed(self, user_agent, url):
        """Check if URL is allowed for user agent"""
        return self.parser.is_allowed(user_agent, url)

    def parse_robots_txt(self, content):
        """Parse robots.txt content manually"""
        lines = content.splitlines()
        rules = []
        current_agent = None
        sitemaps = []
        crawl_delay = 5
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            parts = line.split(':', 1)
            if len(parts) != 2:
                continue
                
            key = parts[0].lower().strip()
            value = parts[1].strip()
            
            if key == 'user-agent':
                current_agent = value
            elif key == 'disallow' and (current_agent == '*' or current_agent == self.user_agent):
                rules.append(('disallow', value))
            elif key == 'allow' and (current_agent == '*' or current_agent == self.user_agent):
                rules.append(('allow', value))
            elif key == 'sitemap':
                sitemaps.append(value)
            elif key == 'crawl-delay':
                try:
                    crawl_delay = float(value)
                except:
                    pass
        
        return rules, sitemaps, crawl_delay

    def check_rules(self, rules, url):  # Renamed from is_allowed
        """Check if URL is allowed based on parsed rules"""
        path = urlparse(url).path
        
        # Find most specific rule that applies
        most_specific_length = -1
        most_specific_rule = 'allow'  # Default to allow
        
        for rule_type, rule_path in rules:
            if rule_path == '/':  # Special case for root
                if most_specific_length < 0:
                    most_specific_length = 0
                    most_specific_rule = rule_type
            elif path.startswith(rule_path):
                if len(rule_path) > most_specific_length:
                    most_specific_length = len(rule_path)
                    most_specific_rule = rule_type
        
        return most_specific_rule == 'allow'

class Crawler:
    def __init__(self, seed_urls, max_pages=100):
        self.db = Database()
        self.frontier = seed_urls.copy()
        self.visited = set()
        self.max_pages = max_pages
        self.user_agent = "fri-wier-FTL"
        self.headers = {'User-Agent': self.user_agent}
        self.robots_cache = {}  # Cache for robots.txt
        self.min_hash_cache = set() 
        
    def start(self):
        # Add seed URLs to frontier pages in database
        print(f"Worker starting - Adding {len(self.frontier)} seed URLs to frontier")
        
        # Remove sitemap URLs that might cause infinite loops
        if hasattr(self.db, 'remove_sitemap_urls_from_frontier'):
            self.db.remove_sitemap_urls_from_frontier()
        
        for url in self.frontier:
            success = self.db.add_page_to_frontier(url, priority=0)
            print(f"Added {url} to frontier: {success}")
        
        print("Starting crawling process...")
        pages_crawled = 0
        empty_frontier_count = 0  # Track consecutive empty frontier results
        
        while pages_crawled < self.max_pages:
            # IMPORTANT: Use preferential method to get next URL
            next_url = self.db.get_next_frontier_page_preferential()
            
            if not next_url:
                empty_frontier_count += 1
                print(f"No URLs in frontier! Attempt {empty_frontier_count}/5")
                if empty_frontier_count >= 5:
                    print("No more URLs in frontier after 5 attempts! Exiting.")
                    break
                # Sleep briefly before trying again
                time.sleep(5)
                continue
                
            # Reset counter when we get a URL
            empty_frontier_count = 0
            
            print(f"Got next URL from frontier: {next_url}")
            
            # Mark the page as being processed in the database
            self.db.mark_page_as_processing(next_url)
            
            if next_url in self.visited:
                print(f"Skipping already visited URL: {next_url}")
                continue
                    
            print(f"Crawling ({pages_crawled+1}/{self.max_pages}): {next_url}")
            self.crawl_page(next_url)
            self.visited.add(next_url)
            pages_crawled += 1
            
            # Sleep according to robots.txt crawl-delay if available
            if pages_crawled < self.max_pages:
                # Get appropriate delay from robots.txt
                robots_parser = self.get_robots_parser(next_url)
                delay = getattr(robots_parser, 'crawl_delay', 5)
                print(f"Sleeping for {delay} seconds (per robots.txt)...")
                time.sleep(delay)
        
        print(f"Worker finished after crawling {pages_crawled} pages")
    
    def priority(self, link_tag):
        """
        Compute the priority of a link.

        Args:
            html (str): HTML content of the page.
            link (str): Link URL.
            link_tag (bs4.Tag): BeautifulSoup tag representing the link.

        Returns:
            float: Priority score (lower number represents high priority).
        """
        highest_similarity = 0.0
        for keyword in self.db.preferential_keywords:
            window_size = 50
            # Get the content of the parent tag to the link
            sourounding_text = link_tag.parent.text
            #if not sourounding_text.strip():
            #    return 1  # Low priority

            index = sourounding_text.find(link_tag.text)
            start = max(0, index - window_size)
            end = min(len(sourounding_text), index + window_size)
            sourounding_text = sourounding_text[start:end]

            # Create Bag of Words representations
            vectorizer = CountVectorizer(stop_words='english')
            texts = [keyword, sourounding_text]
            word_vectors = vectorizer.fit_transform(texts)

            # Compute cosine similarity between the two bags of words
            similarity = cosine_similarity(word_vectors[0], word_vectors[1])[0][0]

            if similarity > highest_similarity:
                highest_similarity = similarity

        
        # Similarity is biggest at 1.0, but priority is lowest at 1.0
        priority = 1 - highest_similarity
        return priority
    
    def get_robots_parser(self, url):
        """Get robots parser for the domain with improved sitemap detection"""
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        domain = parsed_url.netloc
        
        if (base_url in self.robots_cache):
            return self.robots_cache[base_url]
        
        robots_url = f"{base_url}/robots.txt"
        try:
            response = requests.get(robots_url, headers=self.headers, timeout=5)
            if response.status_code == 200:
                robots_content = response.text
                print(f"  Found robots.txt for {domain}")
                
                # Store in database
                self.db.add_site(parsed_url.netloc, robots_content)
                
                # Create custom parser that accepts additional attributes
                custom_parser = CustomRobotsParser()
                custom_parser.parse(robots_content)
                
                # Extract sitemaps
                for line in robots_content.splitlines():
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        custom_parser.sitemaps.append(sitemap_url)
                
                # Extract crawl delay
                for line in robots_content.splitlines():
                    if line.lower().startswith('crawl-delay:'):
                        try:
                            delay = float(line.split(':', 1)[1].strip())
                            if delay > 0:
                                custom_parser.crawl_delay = delay
                        except:
                            pass
                
                self.robots_cache[base_url] = custom_parser
                return custom_parser
            else:
                # Empty parser if no robots.txt
                print(f"  No robots.txt found for {domain} (status: {response.status_code})")
                custom_parser = CustomRobotsParser()
                self.robots_cache[base_url] = custom_parser
                return custom_parser
        except Exception as e:
            print(f"  Error fetching robots.txt for {domain}: {e}")
            # In case of error, assume everything is allowed
            custom_parser = CustomRobotsParser()
            self.robots_cache[base_url] = custom_parser
            return custom_parser
            
    def is_crawlable(self, url):
        """Check if URL is crawlable according to robots.txt with binary content exception"""
        try:
            # Always allow binary content URLs based on extension
            extension = url.split('.')[-1].lower() if '.' in url else ''
            if extension in ['pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx']:
                print(f"  Allowing binary file: {url}")
                return True
                
            robots_parser = self.get_robots_parser(url)
            
            # Debug the rules being applied
            parsed_url = urlparse(url)
            path = parsed_url.path
            
            # Try the proper method with debugging output
            result = robots_parser.is_allowed("*", url)
            
            # Print detailed debugging info
            print(f"  Robots check for {url}: {result}")
            print(f"  Path being checked: {path}")
            
            return result
        except Exception as e:
            print(f"  Error checking if URL is crawlable: {e}")
            # If there's an error, assume it's allowed to crawl
            return True
        
    def canonicalize_url(self, url):
        """Canonicalize URL to avoid duplicates"""
        parsed = urlparse(url)
        
        # Lowercase the scheme and netloc
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        
        # Remove fragments
        path = parsed.path
        params = parsed.params
        query = parsed.query
        fragment = ''  # Remove fragment
        
        # Remove trailing slash from path if present (except root)
        if path.endswith('/') and path != '/':
            path = path[:-1]
            
        # Rebuild the URL
        canonical = urlunparse((scheme, netloc, path, params, query, fragment))
        
        return canonical
        
    def compute_content_hash(self, content):
        """Compute hash of content to detect duplicates"""
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def compute_minhash_signature(self, html_content):
        """Compute MinHash signature for the content"""
        content = self.preprocess_html(html_content)
        minhash = MinHash()
        
        # Tokenize the content
        tokens = content.split()
        
        # Update MinHash with the tokens
        for token in tokens:
            minhash.update(token.encode('utf8'))
        
        hashable_minhash =  base64.b64encode(minhash.digest().tobytes()).decode("utf-8") 
        self.min_hash_cache.add(hashable_minhash) 
        print(f"  MinHash signature hashable minhash: {hashable_minhash}")
        return hashable_minhash
    
    def preprocess_html(self, html_content):
        # Parse HTML and extract meaningful text
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style tags
        for script_or_style in soup(['script', 'style']):
            script_or_style.decompose()
        
        # Get text content (you can also tokenize here if needed)
        text_content = soup.get_text().strip()
        return text_content
    
    def compare_minhash(self, minhash1, minhash2):
        # Compare the Jaccard similarity based on the Minhash signatures
        similarity = minhash1.jaccard(minhash2)
        if similarity > 0.8:
            return True 
        else:
            return False
    
    def check_minhash_exists(self, minhash):
        """Check if MinHash exists in the cache"""
        minhash_signature = minhash
        
        for existing_signature in self.min_hash_cache:
            if self.compare_minhash(existing_signature, minhash_signature):
                duplicate_id, duplicate_url = self.db.get_duplicate_page_by_minhash(existing_signature)
                return duplicate_id, duplicate_url
                
        return None, None

    def extract_links(self, html_content, base_url):
        """Extract links from HTML content with priority information"""
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        
        # Process all anchor tags
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
                
            # Normalize URL
            full_url = urljoin(base_url, href)
            canonical_url = self.canonicalize_url(full_url)
            
            # Get priority using surrounding context
            priority = self.priority(a_tag)
            
            # Return links with priority
            links.append((canonical_url, priority))
        
        return links
        
    def extract_images(self, html_content, base_url, page_id):
        """Extract images from HTML with full information"""
        try:
            # Use BeautifulSoup for better parsing
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Track processed URLs to avoid duplicates
            processed_img_urls = set()
            
            # Extract standard img tags
            for img in soup.find_all('img', src=True):
                img_url = img.get('src', '').strip()
                if not img_url:
                    continue
                    
                # Resolve relative URLs
                img_url = urljoin(base_url, img_url)
                
                if img_url in processed_img_urls:
                    continue
                    
                processed_img_urls.add(img_url)
                
                # Check if this is a data URL
                if img_url.startswith('data:'):
                    try:
                        # Extract mime type and generate a filename
                        mime_parts = img_url.split(';')[0].split(':')
                        if len(mime_parts) > 1:
                            content_type = mime_parts[1]
                            extension = content_type.split('/')[-1]
                            filename = f"inline-{hash(img_url)}.{extension}"
                            
                            # Log that we're skipping download but storing metadata
                            print(f"  Skipping download of data URL (storing metadata only)")
                            
                            # Store image metadata (without data)
                            self.db.add_image(page_id, filename, content_type, None)
                    except Exception as e:
                        print(f"  Error processing data URL: {e}")
                    continue
                
                # Get image filename from URL and truncate if needed
                parsed = urlparse(img_url)
                filename = os.path.basename(parsed.path)
                
                if not filename:
                    continue
                    
                # Truncate filename if too long (database column limit is 50 chars)
                if len(filename) > 45:
                    name, ext = os.path.splitext(filename)
                    filename = name[:40] + "..." + ext if ext else name[:45]
                    
                try:
                    # Download image with timeout and proper headers
                    img_response = requests.get(
                        img_url, 
                        headers=self.headers, 
                        timeout=5,
                        stream=True  # Stream to handle large files
                    )
                    
                    if img_response.status_code == 200:
                        content_type = img_response.headers.get('Content-Type', '')
                        
                        # Get image data (first 1MB max for storage)
                        img_data = None
                        if len(img_response.content) < 1024*1024:  # 1MB limit
                            img_data = img_response.content
                            
                        # Store image in database
                        self.db.add_image(page_id, filename, content_type, img_data)
                        print(f"  Stored image: {filename} ({content_type})")
                        
                except Exception as e:
                    print(f"  Error downloading image {img_url}: {e}")
            
            # Also extract CSS background images
            for tag in soup.find_all(style=True):
                style = tag.get('style', '')
                for url in re.findall(r'url\([\'"]?(.*?)[\'"]?\)', style):
                    if not url or url in processed_img_urls:
                        continue
                        
                    img_url = urljoin(base_url, url)
                    processed_img_urls.add(img_url)
                    
                    # Get filename
                    parsed = urlparse(img_url)
                    filename = os.path.basename(parsed.path)
                    
                    if not filename:
                        continue
                        
                    try:
                        img_response = requests.get(img_url, headers=self.headers, timeout=5)
                        if img_response.status_code == 200:
                            content_type = img_response.headers.get('Content-Type', '')
                            self.db.add_image(page_id, filename, content_type, None)
                    except Exception as e:
                        print(f"  Error downloading CSS image {img_url}: {e}")
                        
        except Exception as e:
            print(f"  Error extracting images: {e}")
                
    def is_binary_content(self, content_type):
        """Check if content is binary"""
        binary_types = {
            'application/pdf': 'PDF',
            'application/msword': 'DOC',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'DOCX',
            'application/vnd.ms-powerpoint': 'PPT',
            'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'PPTX',
            'application/vnd.ms-excel': 'XLS',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'XLSX',
            'application/zip': 'ZIP',
            'application/x-rar': 'RAR',
            'application/x-rar-compressed': 'RAR',
            'application/octet-stream': 'BIN',
            'application/x-7z-compressed': '7Z',
            'application/x-tar': 'TAR',
            'application/x-pdf': 'PDF',
            'image/tiff': 'TIFF'
        }
        
        # Strip any parameters from content type
        if ';' in content_type:
            content_type = content_type.split(';')[0].strip().lower()
        else:
            content_type = content_type.lower()
        
        return binary_types.get(content_type, None)
        
    def crawl_page(self, url):
        """Crawl a single page and extract links, images, etc."""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        # At the beginning of your crawl_page method:
        if self.skip_visited_url(url):
            return  # Exit immediately if already visited

        # Process sitemap on the first page from each domain
        if not hasattr(self, 'processed_sitemap_domains'):
            self.processed_sitemap_domains = set()

        # Extract and clean domain for consistent comparison
        clean_domain = domain
        if '@' in clean_domain or clean_domain == '://' or len(clean_domain) < 3:
            print(f"Skipping sitemap for invalid domain: {domain}")
        else:
            if clean_domain not in self.processed_sitemap_domains:
                print(f"First time seeing domain {clean_domain}, processing sitemap...")
                self.process_sitemap(f"{parsed_url.scheme}://{clean_domain}/")
                self.probe_for_binary_content(clean_domain)
                self.processed_sitemap_domains.add(clean_domain)
        
        # Check robots.txt
        if not self.is_crawlable(url):
            print(f"  Skipping (robots.txt): {url}")
            # Remove from processing but don't mark as duplicate or disallowed
            with Database._processing_lock:
                if hasattr(self.db, 'processing_pages') and url in self.db.processing_pages:
                    self.db.processing_pages.remove(url)
            return
        
        try:
            # Fetch the page with stream=True for binary content
            response = requests.get(url, headers=self.headers, timeout=10, stream=True)
            
            # Debug the response content-type
            content_type = response.headers.get('Content-Type', '')
            print(f"  Content-Type: {content_type}")
            
            # Check content type (improved to better catch PDFs)
            clean_content_type = content_type.split(';')[0].strip().lower()
            
            # Handle binary content with detailed diagnostics
            binary_type = self.is_binary_content(clean_content_type)
            
            # Also check URL extension for binary content
            if not binary_type and '.' in url:
                extension = url.split('.')[-1].lower()
                if extension in ['pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx']:
                    if extension == 'pdf':
                        binary_type = 'PDF'
                    elif extension in ['doc', 'docx']:
                        binary_type = 'DOC'
                    elif extension in ['ppt', 'pptx']:
                        binary_type = 'PPT'
                    elif extension in ['xls', 'xlsx']:
                        binary_type = 'XLS'
            
            if binary_type:
                print(f"  Detected binary content type: {binary_type}")
                # Store binary data
                binary_data = response.content
                # Update page as binary
                page_id = self.db.update_page(url, None, response.status_code, 'BINARY')
                print(f"  Created binary page with ID: {page_id}")
                
                # Store binary content with verification
                success = self.db.add_binary_content(page_id, binary_type, binary_data)
                if success:
                    print(f"  Successfully stored binary content ({binary_type}): {url}")
                else:
                    print(f"  Failed to store binary content: {url}")
                return
                
            # Handle HTML content
            if 'text/html' in content_type:
                html_content = response.text
                # Check for duplicate 
                content_hash = hashlib.md5(html_content.encode('utf-8')).hexdigest()
                duplicate_id, duplicate_url = self.db.check_content_hash_exists(content_hash)
                
                if duplicate_id and duplicate_url != url:
                    # Mark as duplicate and return
                    print(f"Found duplicate: {url} matches {duplicate_url}")
                    self.db.mark_as_duplicate(url, duplicate_url)
                    return True  # Count as success since we processed it
                    
                # Process as normal HTML
                # Store content
                if html_content and len(html_content.strip()) > 0:
                    content_hash = self.compute_content_hash(html_content)
                    content_minhash = self.compute_minhash_signature(html_content)
                    page_id = self.db.update_page_with_hash_and_minhash(url, html_content, response.status_code, content_hash, content_minhash)
                    
                    # Debug successful storage
                    print(f"  Stored HTML content ({len(html_content)} bytes) with hash: {content_hash[:8]} and minhash{content_minhash[:6]}...")
                    
                    # Check if this is a duplicate
                    hash_dup_id, hash_dup_url = self.db.check_content_hash_exists(content_hash)
                    minhash_dup_id, minhash_dup_url = self.check_minhash_exists(content_minhash)
                    
                    # Use separate variables and check both results
                    if (hash_dup_id and hash_dup_id != page_id) or (minhash_dup_id and minhash_dup_id != page_id):
                        # Use whichever duplicate was found
                        found_dup_id = hash_dup_id or minhash_dup_id
                        found_dup_url = hash_dup_url or minhash_dup_url
                        print(f"  Found duplicate content: {url} matches {found_dup_url}")
                        self.db.mark_as_duplicate(url, found_dup_url)
                        return True
                else:
                    print(f"  WARNING: Empty HTML content for {url}")
                    # Still update the page status even if content is empty
                    page_id = self.db.update_page(url, None, response.status_code, 'HTML')
                
                # Extract links and process them
                links = self.extract_links(html_content, url)
                print(f"  Extracted {len(links)} links from {url}")
                
                for link_url, priority in links:
                    # Add to database frontier
                    self.db.add_page_to_frontier(link_url, priority)
                    
                    # Add to shared queue if available
                    if hasattr(self, 'url_queue'):
                        self.url_queue.add_url(link_url, priority)
                    
                    # Store link relationship
                    self.db.add_link(url, link_url)
                
                # Extract images
                self.extract_images(html_content, url, page_id)
                return True
            else:
                # Unknown content type
                print(f"  Unknown content type: {content_type}")
                page_id = self.db.update_page(url, None, response.status_code, 'HTML')
                return page_id
                
        except Exception as e:
            print(f"  Error crawling {url}: {e}")
            # Mark as FRONTIER again in case of error - using a valid page type
            if url not in self.visited:
                self.db.add_page_to_frontier(url)
            # Don't try to mark as "ERROR" since it's not a valid page type
            return None

    def process_sitemap(self, base_url):
        """Process sitemap for the domain with improved processing"""
        domain = urlparse(base_url).netloc
        print(f"\nSearching for sitemaps on domain: {domain}")
        
        found_sitemap = False
        
        # FIRST: Check if we already have sitemap URLs from robots.txt
        robots_parser = self.get_robots_parser(base_url)
        if hasattr(robots_parser, 'sitemaps') and robots_parser.sitemaps:
            print(f"  Found {len(robots_parser.sitemaps)} sitemap(s) in robots.txt")
            
            # Process each sitemap from robots.txt
            for sitemap_url in robots_parser.sitemaps:
                try:
                    print(f"  Processing sitemap from robots.txt: {sitemap_url}")
                    response = requests.get(sitemap_url, headers=self.headers, timeout=10)
                    
                    if response.status_code == 200:
                        # Process the sitemap content
                        sitemap_content = response.text
                        success = self.db.update_site_sitemap(domain, sitemap_content)
                        print(f"  Sitemap storage result: {success}")
                        
                        # Process sitemap contents
                        if '<url>' in sitemap_content or '<loc>' in sitemap_content:
                            found_sitemap = True
                            self.process_sitemap_content(sitemap_content, sitemap_url, domain)
                except Exception as e:
                    print(f"  Error processing sitemap from robots.txt: {e}")
        
        # Only try common paths if no sitemaps found in robots.txt
        if not found_sitemap:
            # Your existing code for common sitemap paths
            # ...
            # Common sitemap paths to check
            sitemap_paths = [
                "sitemap.xml",
                "sitemap_index.xml",
                "sitemap/sitemap.xml", 
                "sitemaps/sitemap.xml",
                "wp-sitemap.xml",         # WordPress sitemaps
                "sitemap-index.xml",      # Add more variations
                "wp-sitemap-index.xml",   # WordPress specific format
                "main-sitemap.xml",       # Another common format
                "forum-sitemap.xml"       # Med.over.net specific sitemap
            ]
            
            found_sitemap = False
            for path in sitemap_paths:
                sitemap_url = urljoin(base_url, path)
                try:
                    print(f"  Trying sitemap URL: {sitemap_url}")
                    response = requests.get(
                        sitemap_url, 
                        headers=self.headers, 
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        sitemap_content = response.text
                        
                        # Check if it's a valid sitemap
                        if '<url>' in sitemap_content or '<loc>' in sitemap_content:
                            print(f"  Found valid sitemap at {sitemap_url}")
                            found_sitemap = True
                            
                            # Store sitemap content in database
                            success = self.db.update_site_sitemap(domain, sitemap_content)
                            print(f"  Sitemap storage result: {success}")
                            
                            # Parse sitemap and extract URLs
                            try:
                                soup = BeautifulSoup(sitemap_content, 'xml')
                                urls = [loc.text.strip() for loc in soup.find_all('loc')]
                                print(f"  Found {len(urls)} URLs in sitemap")
                                
                                # Process sitemap index (nested sitemaps)
                                if "<sitemapindex" in sitemap_content:
                                    print("  This is a sitemap index, processing nested sitemaps...")
                                    for nested_url in urls:
                                        nested_response = requests.get(nested_url, headers=self.headers, timeout=10)
                                        if nested_response.status_code == 200:
                                            nested_content = nested_response.text
                                            nested_soup = BeautifulSoup(nested_content, 'xml')
                                            nested_urls = [loc.text.strip() for loc in nested_soup.find_all('loc')]
                                            print(f"    Found {len(nested_urls)} URLs in nested sitemap")
                                            
                                            # Add these URLs to frontier
                                            for url in nested_urls:
                                                self.db.add_page_to_frontier(url)
                                else:
                                    # Regular sitemap, add all URLs to frontier
                                    for url in urls:
                                        self.db.add_page_to_frontier(url)
                            except Exception as e:
                                print(f"  Error parsing sitemap: {e}")
                            
                            break  # Stop checking other paths if we found a sitemap
                        else:
                            print(f"  Not a valid sitemap format at {sitemap_url}")
                except Exception as e:
                    print(f"  Error checking sitemap at {sitemap_url}: {e}")
            
            if not found_sitemap:
                print(f"  No sitemap found for domain {domain}")

    def process_sitemap_content(self, sitemap_content, sitemap_url, domain):
        """Process the content of a sitemap XML file"""
        if '<url>' in sitemap_content or '<loc>' in sitemap_content:
            print(f"  Found valid sitemap at {sitemap_url}")
            
            try:
                soup = BeautifulSoup(sitemap_content, 'xml')
                urls = [loc.text.strip() for loc in soup.find_all('loc')]
                print(f"  Found {len(urls)} URLs in sitemap")
                
                # Process sitemap index (nested sitemaps)
                if "<sitemapindex" in sitemap_content:
                    print("  This is a sitemap index, processing nested sitemaps...")
                    for nested_url in urls:
                        nested_response = requests.get(nested_url, headers=self.headers, timeout=10)
                        if nested_response.status_code == 200:
                            nested_content = nested_response.text
                            nested_soup = BeautifulSoup(nested_content, 'xml')
                            nested_urls = [loc.text.strip() for loc in nested_soup.find_all('loc')]
                            print(f"    Found {len(nested_urls)} URLs in nested sitemap")
                            
                            # Add these URLs to frontier
                            for url in nested_urls:
                                self.db.add_page_to_frontier(url)
                else:
                    # Regular sitemap, add all URLs to frontier
                    for url in urls:
                        self.db.add_page_to_frontier(url)
                return True
            except Exception as e:
                print(f"  Error parsing sitemap: {e}")
        
        return False

    def probe_for_binary_content(self, domain):
        """Explicitly probe for common binary files on domain"""
        base_url = f"https://{domain}"
        
        # Expanded list of common binary content locations
        binary_paths = [
            # Generic site PDF paths
            "/terms.pdf",
            "/privacy.pdf", 
            "/download.pdf",
            "/report.pdf",
            
            # Med.over.net specific paths
            "/wp-content/uploads/2020/01/sample.pdf",
            "/wp-content/uploads/report.pdf",
            
            # Common media paths
            "/media/files/brochure.pdf",
            "/documents/info.pdf"
        ]
        
        print(f"Probing for binary content on {domain}...")
        
        for path in binary_paths:
            try:
                url = base_url + path
                print(f"  Checking for binary at: {url}")
                
                # Just add to frontier - full processing will happen later
                self.db.add_page_to_frontier(url)
            except Exception as e:
                pass
                
    def skip_visited_url(self, url):
        """Check if URL has been visited and properly remove it from frontier if so"""
        cursor = self.db.conn.cursor()
        try:
            # Check if this URL has been processed with any page_type other than FRONTIER
            cursor.execute("""
                SELECT COUNT(*) 
                FROM crawldb.page 
                WHERE url = %s AND page_type_code != 'FRONTIER'
            """, (url,))
            
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"Skipping already visited URL: {url}")
                
                # CRITICAL: Remove from frontier if already visited
                cursor.execute("""
                    DELETE FROM crawldb.page
                    WHERE url = %s AND page_type_code = 'FRONTIER'
                """, (url,))
                
                self.db.conn.commit()
                return True
            return False
        finally:
            cursor.close()

    # Add a depth parameter to prevent infinite recursion
    def process_sitemap_url(self, sitemap_url, depth=0):
        if depth > 3:  # Max recursion depth
            return []
        # Processing logic here
        if "<sitemapindex" in content:
            return self.process_sitemap_url(nested_url, depth+1)

    def analyze_link_context(self, html_content, link, surrounding_window=50):
        """
        Analyze the context around a link to determine its priority
        based on proximity to preferential keywords
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the link tag
        link_tags = soup.find_all('a', href=True)
        for link_tag in link_tags:
            if link_tag.get('href') in link or link in link_tag.get('href'):
                # Get surrounding text
                parent_text = link_tag.parent.text
                link_text = link_tag.text
                
                if not hasattr(self.db, 'preferential_keywords'):
                    return 0
                    
                # Check if any keyword is in surrounding text
                for keyword in self.db.preferential_keywords:
                    if keyword.lower() in parent_text.lower():
                        return 1  # Higher priority
                        
        return 0  # Normal priority

    def add_urls_to_frontier(self, links, source_url):
        """Add URLs to frontier with priority information"""
        # Get domain of source URL for domain filtering
        source_domain = urlparse(source_url).netloc
        
        for url, priority in links:
            parsed = urlparse(url)
            target_domain = parsed.netloc
            
            # Skip already visited URLs
            if url in self.visited:
                continue
                
            # Add to frontier with priority
            self.db.add_page_to_frontier(url, priority)

    def find_duplicates(self):
        """Force comparison of all pages to detect duplicates"""
        cursor = self.db.conn.cursor()
        try:
            # Get all HTML pages with content hash
            cursor.execute("""
                SELECT id, url, content_hash, content_minhash 
                FROM crawldb.page 
                WHERE page_type_code = 'HTML' 
                AND (content_hash IS NOT NULL OR content_minhash IS NOT NULL)
            """)
            
            pages = cursor.fetchall()
            print(f"Checking {len(pages)} pages for duplicates...")
            
            # Compare each pair of pages
            for i in range(len(pages)):
                for j in range(i+1, len(pages)):
                    page1_id, page1_url, page1_hash, page1_minhash = pages[i]
                    page2_id, page2_url, page2_hash, page2_minhash = pages[j]
                    
                    # Check hash match
                    if page1_hash and page2_hash and page1_hash == page2_hash:
                        print(f"Hash match: {page1_url} and {page2_url}")
                        self.db.mark_as_duplicate(page2_url, page1_url)
                    # Check minhash match
                    elif page1_minhash and page2_minhash and page1_minhash == page2_minhash:
                        print(f"MinHash match: {page1_url} and {page2_url}")
                        self.db.mark_as_duplicate(page2_url, page1_url)
        finally:
            cursor.close()