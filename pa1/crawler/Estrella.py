import os
import time
import hashlib
import requests
import dotenv
import heapq
import random
import re
import numpy as np
from urllib.parse import urljoin, urlparse, urlsplit
import urllib.robotparser
from bs4 import BeautifulSoup
from threading import Thread, Lock
from datetime import datetime
from Connection import PostgresDB
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from mimetypes import guess_extension, guess_type
import mimetypes
import requests
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

dotenv.load_dotenv()
# Override database name to use VectorDB01
db_name = "VectorDB01"
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")


class SitemapFetcher:
    def __init__(self, domain):
        self.domain = domain.rstrip('/')
        self.sitemap_urls = []
        self.common_sitemap_paths = [
            "sitemap.xml", "sitemap_index.xml", "sitemap/sitemap.xml", 
            "sitemaps/sitemap.xml", "wp-sitemap.xml", "sitemap-index.xml", 
            "wp-sitemap-index.xml", "main-sitemap.xml", "forum-sitemap.xml"
        ]
    
    def fetch_sitemap(self):
        """Find and fetch the sitemap content, extracting all URLs."""
        robots_url = urljoin(self.domain, "/robots.txt")
        try:
            response = requests.get(robots_url, timeout=5)
            if response.status_code == 200:
                for line in response.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sitemap_url = line.split(":", 1)[1].strip()
                        self.sitemap_urls.append(sitemap_url)
        except Exception as e:
            print(f"Error fetching robots.txt: {e}")

        if not self.sitemap_urls:
            for path in self.common_sitemap_paths:
                potential_url = urljoin(self.domain, path)
                if self._is_valid_sitemap(potential_url):
                    self.sitemap_urls.append(potential_url)
    
    def _is_valid_sitemap(self, url):
        """Check if a given URL is a valid sitemap by attempting to fetch and parse it."""
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200 and "<urlset" in response.text:
                return True
        except:
            pass
        return False
    
    def extract_urls(self):
        """Extract all URLs from the found sitemap(s)."""
        all_urls = []
        for sitemap_url in self.sitemap_urls:
            try:
                response = requests.get(sitemap_url, timeout=5)
                if response.status_code == 200:
                    root = ET.fromstring(response.text)
                    for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                        all_urls.append(elem.text)
            except Exception as e:
                print(f"Error parsing {sitemap_url}: {e}")
        return all_urls

class MinHash:
    def __init__(self, num_hashes=200):
        self.num_hashes = num_hashes
        self.hash_functions = [self._generate_hash_function() for _ in range(num_hashes)]

    def _generate_hash_function(self):
        """Generate a random hash function."""
        a = random.randint(1, 2**32 - 1)
        b = random.randint(0, 2**32 - 1)
        return lambda x: (a * x + b) % (2**32)

    def _hash(self, x, func):
        """Apply a hash function to a value."""
        return func(x)

    def get_signature(self, tokens):
        """Generate the MinHash signature for a set of tokens."""
        signature = []
        for func in self.hash_functions:
            min_hash = min([self._hash(hash(token), func) for token in tokens])
            signature.append(min_hash)
        return signature
    
class Estrella:
    def __init__(self, domain, workers=4, max_pages=5000):
        print(f"Initializing Estrella with URL: {domain}, max_depth: {workers}, max_pages: {max_pages}")
        self.domain = domain.rstrip('/')  # Ensure no trailing slash
        self.workers = workers
        self.max_pages = max_pages
        self.page_count = 0
        self.keywords = ["erasmus", "mednarodna", "izmenjava", "program", "mobilnost", "mednarodna izmenjava", "mednarodna mobilnost", "prijave na erasmus", "prijave na izmenjavo", "prijave na mobilnost", "prijave na erasmus+", "prijave na izmenjavo+", "prijave na mobilnost+", "prijave na erasmus program", "prijave na izmenjavo program", "prijave na mobilnost program"]
        self.user_agent = "FRI-weir-BabaVanga"
        self.header = {'User-Agent': self.user_agent}
        self.robots_chache = {}
        self.request_rate = 5
        self.sitemap_chache = {}
        self.visited_urls = set()
        self.lock = Lock()
        self.urls_in_queue = set()
        self.queue = []
        self.page_hashes = set()
        self.minhash_dict = {}

        self.init_db()
        self.load_visited_urls()  # Load previously visited URLs
        self.init_robots_parser()
        self.init_sitemap_parser()
        self.minhasher = MinHash(num_hashes=200)
        
        # Initialize queue with unvisited URLs
        self.seed_initial_urls()
        print(f"Initial queue size: {len(self.queue)}")

    def init_db(self):
        self.db = PostgresDB(db_name, db_user, db_password, db_host, db_port)
        self.db.connect()
        self.db._init_schema()

    def init_robots_parser(self):

        self.robots_parser = urllib.robotparser.RobotFileParser()
        self.robots_parser.set_url(urljoin(self.domain, "/robots.txt"))
        self.robots_parser.read()
        self.crawl_delay = self.robots_parser.crawl_delay(self.user_agent)
        self.sitemap_urls = self.robots_parser.site_maps()
        print("Crawl delay:", self.crawl_delay)
        print("Sitemap URLs:", self.sitemap_urls)
        print("Crawling on domain allowed?", self.robots_parser.can_fetch(self.user_agent, self.domain))

        try:
            with urllib.request.urlopen(urljoin(self.domain, "/robots.txt")) as response:
                self.robots_content = response.read().decode("utf-8")
        except Exception as e:
            print(f"Error fetching robots.txt: {e}")
            self.robots_content = None

    def is_url_allowed_in_robots(self, url):
        return self.robots_parser.can_fetch(self.user_agent, url)
    
    def init_sitemap_parser(self):
        if not self.sitemap_urls:
            sitemap_fetcher = SitemapFetcher(self.domain)
            sitemap_fetcher.fetch_sitemap()
            self.sitemap_urls = sitemap_fetcher.extract_urls()
            self.sitemap_content = str(sitemap_fetcher.sitemap_urls)
            print("Sitemap URLs:", sitemap_fetcher.sitemap_urls)

    def in_domain(self, url):
        return url.startswith(self.domain)
    
    def crawl(self):
        """Main crawling method"""
        # First check if the domain is allowed by robots.txt
        if not self.is_url_allowed_in_robots(self.domain):
            print(f"Domain {self.domain} is not allowed by robots.txt")
            return

        # Get or create site ID
        try:
            # Try to get existing site ID first
            self.cursor = self.db.conn.cursor()
            self.cursor.execute("SELECT id FROM crawldb.site WHERE domain = %s", (self.domain,))
            result = self.cursor.fetchone()
            
            if result:
                self.site_id = result[0]
                print(f"Found existing site ID: {self.site_id}")
            else:
                # If site doesn't exist, create new one
                self.site_id = self.db.insert_site(self.domain, self.robots_content, str(self.sitemap_content))
                print(f"Created new site with ID: {self.site_id}")
        except Exception as e:
            print(f"Error getting/creating site: {e}")
            return
        finally:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()

        # Start crawler threads
        threads = []
        for _ in range(self.workers):
            thread = Thread(target=self.crawl_next_page)
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        self.db.close()

    def detect_page_data_type(self, url, html_content, driver):
        """Detects page type (HTML, BINARY, or DUPLICATE) and handles insertion into DB."""
        try:
            page_hash = hashlib.sha256(html_content.encode()).hexdigest()
            if page_hash in self.page_hashes:
                print("Page already visited (duplicate content)")
                return "DUPLICATE", None
            
            content_type = driver.execute_script("return document.contentType")  
            print(f"Detected content type: {content_type}")

            if content_type == "text/html":
                if self.detect_duplicate(html_content, self.site_id):
                    print("Duplicate page detected")
                    return "DUPLICATE", None
                
                return "HTML", html_content

            binary_types = {
            'application/pdf': 'PDF',
            'application/msword': 'DOC',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'DOCX',
            'application/vnd.ms-powerpoint': 'PPT',
            'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'PPTX',
            }


            if content_type in binary_types:
                return "BINARY", binary_types[content_type]  

            return "UNKNOWN", None

        except Exception as e:
            print("Error detecting page type:", e)
            return "ERROR", None
        
    def extract_binary_files_from_html(self, page_id, html_content):
        """
        Extract binary files (e.g., images, PDFs, videos) from HTML content.

        Args:
            html_content (str): The HTML content of the page.

        Returns:
            List of tuples: Each tuple contains (page_id, data_type_code).
        """
        binary_files = []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        binary_tags = soup.find_all(['a', 'embed', 'object', 'iframe'])

        for tag in binary_tags:
            src = tag.get('src') or tag.get('href')  
            if not src:
                continue  

            src_url = urljoin(html_content, src)

            file_type = self.get_file_type(src_url)
            if file_type:
                # Download binary data
                #file_data = self.download_binary_file(src_url)
                #if file_data:
                 #   # Create a unique identifier for the binary file (could be the URL or a hash of the content)
                 #   page_id = hashlib.sha256(src_url.encode()).hexdigest()
                    binary_files.append((page_id, file_type, src_url))
                    self.db.insert_page_data(page_id, file_type, src_url)
                    print(f" -Binary file: {src_url}, Type: {file_type}, Page ID: {page_id}")
        
        return binary_files
    
    def get_file_type(self, file_url):
        """
        Get the file type based on the URL (can also check file extension).
        
        Args:
            file_url (str): URL to the file.
        
        Returns:
            str: File type (e.g., PDF, PNG, etc.).
        """
        mimetype, encoding = mimetypes.guess_type(file_url)
        if mimetype:
            return self.map_mimetype_to_code(mimetype)
        return None
    
    def map_mimetype_to_code(self, mimetype):
        """
        Map the mimetype to a specific data type code.
        
        Args:
            mimetype (str): The mimetype of the file (e.g., 'application/pdf').
        
        Returns:
            str: The corresponding data type code (e.g., 'PDF').
        """
        binary_types = {
            'application/pdf': 'PDF',
            'application/msword': 'DOC',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'DOCX',
            'application/vnd.ms-powerpoint': 'PPT',
            'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'PPTX'
        }

        return binary_types.get(mimetype)
    
    def extract_images(self, html_content, base_url):
        """
        Extracts image URLs from an HTML page and downloads the image data.
        
        :param html_content: The HTML source of the web page.
        :param base_url: The base URL of the website (to resolve relative links).
        :return: List of tuples (filename, content_type, image_data).
        """
        soup = BeautifulSoup(html_content, "html.parser")
        images = []

        for img_tag in soup.find_all('img', src=True):
            img_url = urljoin(base_url, img_tag['src'])  

            try:
                response = requests.get(img_url, timeout=5)  
                if response.status_code == 200:
                    content_type = response.headers.get("Content-Type", "unknown")
                    ext = guess_extension(content_type) or ".jpg"
                    filename = img_url.split("/")[-1] if "." in img_url.split("/")[-1] else f"image{ext}"
                    image_data = response.content

                    images.append((filename, content_type, image_data))
            except Exception as e:
                #print(f"Failed to fetch image {img_url}: {e}")
                pass
        return images
    
    def extract_links(self, html, base_url):
        """Extracts and returns all links from the HTML content, including JavaScript onclick links."""
        
        soup = BeautifulSoup(html, "html.parser")
        links = []

        # Extract from <a> tags
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            
            if href.startswith("http"):
                full_url = href
            elif href.startswith("//"):
                full_url = "https:" + href
            elif href.startswith("/"):
                full_url = urljoin(base_url, href)
            else:
                full_url = urljoin(base_url, href)

            links.append((full_url, a_tag))

        # JavaScript-based links
        for tag in soup.find_all(onclick=True):
            onclick_text = tag["onclick"]
            match = re.search(r"location\.href\s*=\s*[\"'](.*?)[\"']", onclick_text)
            if match:
                js_href = match.group(1)
                if js_href.startswith("http"):
                    full_url = js_href
                elif js_href.startswith("//"):
                    full_url = "https:" + js_href
                elif js_href.startswith("/"):
                    full_url = urljoin(base_url, js_href)
                else:
                    full_url = urljoin(base_url, js_href)

                links.append((full_url, tag))

        return links
    
    def priority(self, html, link, link_tag):
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
        for keyword in self.keywords:
            window_size = 50
            sourounding_text = link_tag.parent.text

            index = sourounding_text.find(link_tag.text)
            start = max(0, index - window_size)
            end = min(len(sourounding_text), index + window_size)
            sourounding_text = sourounding_text[start:end]

            vectorizer = CountVectorizer(stop_words='english')
            texts = [keyword, sourounding_text]
            word_vectors = vectorizer.fit_transform(texts)

            similarity = cosine_similarity(word_vectors[0], word_vectors[1])[0][0]
            if similarity > highest_similarity:
                highest_similarity = similarity
    
        return 1 - highest_similarity
    
    def crawl_next_page(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)

        remaining_pages = self.max_pages - self.page_count
        print(f"Starting crawler thread. Remaining pages to crawl: {remaining_pages}")

        while remaining_pages > 0:
            # If queue is empty, try to find more URLs
            if not self.queue:
                print("Queue is empty, trying to find more URLs...")
                self.seed_initial_urls()
                if not self.queue:
                    print("No more URLs found, exiting thread")
                    break

            print(f"Page count: {self.page_count}/{self.max_pages} (Queue size: {len(self.queue)})")
            
            with self.lock:
                if not self.queue or self.page_count >= self.max_pages:
                    break
                priority, url = heapq.heappop(self.queue)
                self.urls_in_queue.remove(url)

            if not self.in_domain(url) or url in self.visited_urls:
                continue

            with self.lock:
                if self.page_count >= self.max_pages:
                    break
                self.visited_urls.add(url)

            time.sleep(5)  

            try:
                if not self.is_url_allowed_in_robots(url):
                    print(f"  Skipping {url} due to robots.txt")
                    continue

                print(f"Crawling URL: {url}, Priority: {priority}")
                driver.get(url)
                print("Waiting for page to load... 3s")
                time.sleep(3)

                html_content = driver.page_source
                page_type, processed_content = self.detect_page_data_type(url, html_content, driver)
                
                if page_type == "DUPLICATE":
                    continue  

                page_id = self.db.insert_page(site_id=self.site_id,
                                        page_type_code=page_type,
                                        url=url,
                                        html_content=processed_content,
                                        http_status_code=200,
                                        accessed_time=datetime.now())

                if page_type == "HTML":
                    with self.lock:
                        self.page_count += 1
                        remaining_pages = self.max_pages - self.page_count
                        if remaining_pages <= 0:
                            break
                    
                    images = self.extract_images(html_content, url)
                    for filename, content_type, image_data in images:
                        self.db.insert_image(page_id, filename, content_type, image_data, datetime.now())
                    self.extract_binary_files_from_html(page_id, html_content)

                    # Extract and add new links to queue
                    url_parts = urlsplit(url)
                    base_url = url_parts.scheme + "://" + url_parts.netloc
                    links = self.extract_links(html_content, base_url)
                    print(f"  - Found {len(links)} links")

                    # Try to find links in JavaScript onclick events and other attributes
                    soup = BeautifulSoup(html_content, 'html.parser')
                    onclick_links = []
                    for tag in soup.find_all(onclick=True):
                        onclick_text = tag['onclick']
                        urls = re.findall(r'(?:window\.location|location\.href)\s*=\s*[\'"]([^\'"]+)[\'"]', onclick_text)
                        onclick_links.extend(urls)
                    
                    # Add onclick links to the regular links
                    for onclick_url in onclick_links:
                        full_url = urljoin(base_url, onclick_url)
                        if self.in_domain(full_url):
                            links.append((full_url, None))

                    for link, link_tag in links:
                        if link not in self.visited_urls and link not in self.urls_in_queue and self.in_domain(link):
                            priority = self.priority(html_content, link, link_tag) if link_tag else 0.5
                            print(f"  - Link added to queue {link}, Priority: {priority}")
                            with self.lock:
                                heapq.heappush(self.queue, (priority, link))
                                self.urls_in_queue.add(link)

                elif page_type == "BINARY":
                    self.db.insert_page_data(page_id, "BINARY")

            except Exception as e:
                print(f"Error crawling {url}:", e)
                with self.lock:
                    if url in self.visited_urls:
                        self.visited_urls.remove(url)

        driver.quit()
        print(f"Crawler thread finished. Final page count: {self.page_count}")

    def compare_minhash_signature(self, minhash1, minhash2):
        """Compute Jaccard similarity using MinHash signatures."""
        return np.mean([1 if minhash1[i] == minhash2[i] else 0 for i in range(len(minhash1))])

    def detect_duplicate(self, html_content, page_id, num_hashes=200, threshold=0.8):

        """Detect duplicate page using MinHash and Jaccard similarity."""

        tokens = set(html_content.split())  
        
        minhash = MinHash(num_hashes)

        current_signature = minhash.get_signature(tokens)

        for existing_page_id, existing_signature in self.minhash_dict.items():
            similarity = self.compare_minhash_signature(current_signature, existing_signature)
            
            if similarity >= threshold:
                print(f"Duplicate detected between pages {page_id} and {existing_page_id} with similarity {similarity}")
                return True  # Duplicate found
        
        self.minhash_dict[page_id] = current_signature
        print(f"Page {page_id} added to the database.")
        return False 
    
    def load_visited_urls(self):
        """Load previously visited URLs from the database."""
        try:
            # Get all URLs from the database
            urls = self.db.get_all_urls()
            self.visited_urls.update(urls)
            self.page_count = len(self.visited_urls)
            print(f"Loaded {self.page_count} previously visited URLs from database")
        except Exception as e:
            print(f"Error loading visited URLs: {e}")

    def seed_initial_urls(self):
        """Find and add new URLs to crawl that haven't been visited yet."""
        print("Seeding initial URLs...")
        
        # 1. Try homepage first
        if self.domain not in self.visited_urls:
            heapq.heappush(self.queue, (0, self.domain))
            self.urls_in_queue.add(self.domain)
            print(f"Added domain to queue: {self.domain}")

        # 2. Try to get URLs from homepage
        try:
            print("Fetching URLs from homepage...")
            response = requests.get(self.domain, headers=self.header, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all('a', href=True)
                for link in links:
                    url = urljoin(self.domain, link['href'])
                    if (url not in self.visited_urls and 
                        url not in self.urls_in_queue and 
                        self.in_domain(url)):
                        heapq.heappush(self.queue, (0, url))
                        self.urls_in_queue.add(url)
                        print(f"Added URL from homepage: {url}")
        except Exception as e:
            print(f"Error fetching homepage: {e}")

        # 3. Try common paths that might exist
        common_paths = [
            "/en",
            "/sl",
            "/about",
            "/o-fakulteti",
            "/studij",
            "/raziskovanje",
            "/en/about",
            "/en/study",
            "/en/research",
            "/novice",
            "/news",
            "/events",
            "/dogodki",
            "/contact",
            "/kontakt"
        ]
        
        for path in common_paths:
            url = urljoin(self.domain, path)
            if (url not in self.visited_urls and 
                url not in self.urls_in_queue and 
                self.in_domain(url)):
                heapq.heappush(self.queue, (0, url))
                self.urls_in_queue.add(url)
                print(f"Added common path: {url}")

        # 4. Try sitemap again with more paths
        sitemap_fetcher = SitemapFetcher(self.domain)
        sitemap_fetcher.fetch_sitemap()
        sitemap_urls = sitemap_fetcher.extract_urls()
        
        if sitemap_urls:
            print(f"Found {len(sitemap_urls)} URLs in sitemap")
            for url in sitemap_urls:
                if (url not in self.visited_urls and 
                    url not in self.urls_in_queue and 
                    self.in_domain(url)):
                    heapq.heappush(self.queue, (0, url))
                    self.urls_in_queue.add(url)
                    print(f"Added URL from sitemap: {url}")

        if not self.queue:
            print("WARNING: Could not find any new URLs to crawl!")
        else:
            print(f"Successfully added {len(self.queue)} new URLs to crawl")

if __name__ == "__main__":
    url = "https://www.fri.uni-lj.si/"  
    workers = 6
    max_pages = 5000

    estrella = Estrella(url, workers, max_pages)
    estrella.crawl()