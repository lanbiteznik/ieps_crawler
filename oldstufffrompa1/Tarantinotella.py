import os
import time
import hashlib
import requests
import dotenv
import heapq
import random
import re
import robotexclusionrulesparser
from urllib.parse import urljoin, urlparse, urlsplit
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from threading import Thread, Lock
from datetime import datetime
from Connection import PostgresDB as BabaVangaDB
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from mimetypes import guess_extension, guess_type

dotenv.load_dotenv()
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

db = BabaVangaDB(db_name, db_user, db_password, db_host, db_port)
db.connect()

class CustomRobotsParser:
    """Wrapper for robotexclusionrulesparser with additional attributes"""
    def __init__(self):
        self.parser = robotexclusionrulesparser.RobotExclusionRulesParser()
        self.sitemaps = []
        self.crawl_delay = 5
    
    def parse(self, content):
        """Parse robots.txt content"""
        return self.parser.parse(content)
    
    def is_user_agent_allowed(self, user_agent, url):
        """Check if user agent is allowed to crawl the URL"""
        return self.parser.is_allowed(user_agent, url)

    def parse_robots_txt(self, content):
        """Parse robots.txt content manually into rules, sitemaps, and crawl delay"""
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

    def is_url_allowed(self, rules, url):
        """Determine if a given URL is allowed based on robots.txt rules."""
        path = urlparse(url).path  
        longest_match_length = -1
        final_decision = True  

        for rule_type, rule_path in rules:
            if path.startswith(rule_path):
                if len(rule_path) > longest_match_length:
                    longest_match_length = len(rule_path)
                    final_decision = (rule_type == "allow")  

        return final_decision  

class SpiderMonkey:
    def __init__(self, domain, max_workers=4, max_pages=5000):
        self.domain = domain
        self.max_workers = max_workers
        self.keyword = "kajenje pomaga zdravju" # nared v keywords.multi
        self.preferential_keywords_or_phrases = ["kava", "cigareti", "alkohol", "zdravje", "kava in cigareti"] #todo use this instead of keyword
        self.user_agent = "FRI-weir-BabaVanga"
        self.headers = {'User-Agent': self.user_agent}
        self.robots_content = None #dont use this one
        self.robots_cache = {} # Cache for robots.txt content
        self.sitemap_content = None
        self.site_id = None
        self.visited_urls = set()
        self.page_hashes = set()
        self.lock = Lock()
        self.url_queue = []
        heapq.heappush(self.url_queue, (0, domain))
        self.max_pages = max_pages
        self.page_count = 0
        self.init_db()

#what pushing in heappush it need to be sorted by relevance and you need to check if it already exists inside, somehow, i would also like to sotre information on where from and where to links but its not that importnat if not . 
#the where from and to might actually be reallly important for visualization 
    def init_db(self):
        self.db = BabaVangaDB(db_name, db_user, db_password, db_host, db_port)
        self.db.connect()

    def in_domain(self, url):
        return url.startswith(self.domain)

    def start_crawler(self):

        print("Can i crawl?:", self.is_crawling_on_url_allowed(self.domain))
        print("sitemap: ", self.get_sitemap_urls(urljoin(self.domain, "/sitemap.xml")))

        if self.can_crawl(self.domain):
            self.site_id = db.insert_site(self.domain, self.robots_content, str(self.sitemap_content))
            heapq.heappush(self.url_queue, (0, self.domain))

        threads = []
        for _ in range(self.max_workers):
            thread = Thread(target=self.crawl_dynamic_page)
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        db.close()
    
    #TALE SITEMAP ŠE MEN NE DELA VREDU; VZEM OD LANEDELRAY, maybe the process sitemap function? 
    def get_sitemap_urls(self, sitemap_url):
        try:
            response = requests.get(sitemap_url, timeout=2)
            print("Sitemap response status:", response.status_code)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "xml")
                self.sitemap_content = soup
                urls = [url.loc.text for url in soup.find_all("loc")]
                return urls
        except Exception as e:
            print("Error fetching sitemap:", e)
        return []
    
    
    def is_crawling_on_url_allowed(self, url):
        """Check if URL is crawlable according to robots.txt with binary content exception"""
        try:
            extension = url.split('.')[-1].lower() if '.' in url else ''
            if extension in ['pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx']:
                print(f" Allowing crawling for binary file: {url}")
                return True
                
            robots_parser = self.get_robots_parser(url)
            result = robots_parser.is_user_agent_allowed(self.user_agent, url)
            print(f"Robots rules for {url}: {result}")
            return result
        
        except Exception as e:
            print(f"  Error checking if URL is crawlable: {e}")
            return True
    
    #todo nevem rn kaj naj bi to delalo pa kaj robots deluje. i think i know liek https//nekineki/nekidruga in če sm v urlju https//nekineki bi e mogu gledat v chached robots če se tega lahko res dotaknem al ga raj skippam ? kapish !!
    #rn pa to dela god knows what 
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

    def extract_links(self, html, base_url):
        """Extracts and returns all links from the HTML content, including JavaScript onclick links."""
        
        soup = BeautifulSoup(html, "html.parser")
        links = []

        # Extract from <a> tags
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            
            # Handle different types of links
            if href.startswith("http"):
                full_url = href
            elif href.startswith("//"):
                full_url = "https:" + href
            elif href.startswith("/"):
                full_url = urljoin(base_url, href)
            else:
                full_url = urljoin(base_url, href)

            links.append((full_url, a_tag))

        # Extract from onclick attributes (JavaScript-based links)
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
            img_url = urljoin(base_url, img_tag['src'])  # Handle relative URLs

            try:
                response = requests.get(img_url, timeout=5)  # Fetch image
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
                return "HTML", html_content

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
                'image/tiff': 'TIFF',
                'image/jpeg': 'JPEG',
                'image/png': 'PNG',
                'audio/mpeg': 'MP3',
                'video/mp4': 'MP4'
            }

            if content_type in binary_types:
                return "BINARY", binary_types[content_type]  #returns the type of the file

            return "UNKNOWN", None

        except Exception as e:
            print("Error detecting page type:", e)
            return "ERROR", None


    def crawl_dynamic_page(self):

        #CHECK WITH ROBOTS.TXT IF CRAWLING IS ALLOWED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)

        while self.url_queue and self.page_count < self.max_pages:
            print("Pagenumber:", self.page_count)
            with self.lock:
                if not self.url_queue:
                    continue
                _, url = heapq.heappop(self.url_queue)

            if not self.in_domain(url) or url in self.visited_urls:
                continue
            self.visited_urls.add(url)

            time.sleep(5)  # Be polite to the server

            try:
                # Check if crawling is allowed
                if not self.is_crawling_on_url_allowed(url):
                    print(f"  Skipping {url} due to robots.txt")
                    continue

                print("Crawling page", url)
                driver.get(url)
                time.sleep(3)

                html_content = driver.page_source
                page_type, processed_content = self.detect_page_data_type(url, html_content, driver)
                
                if page_type == "DUPLICATE": #should i just skip?
                    continue  

                page_id = db.insert_page(site_id=self.site_id,
                                        page_type_code=page_type,
                                        url=url,
                                        html_content=processed_content,
                                        http_status_code=200,
                                        accessed_time=datetime.now())

                if page_type == "HTML":
                    images = self.extract_images(html_content, url)
                    for filename, content_type, image_data in images:
                        db.insert_image(page_id, filename, content_type, image_data, datetime.now())

                elif page_type == "BINARY":
                    # Store binary metadata without content
                    db.insert_page_data(page_id, "BINARY")

                # Extract links for crawling
                url_parts = urlsplit(url)
                base_url = url_parts.scheme + "://" + url_parts.netloc
                links = self.extract_links(html_content, base_url)
                print(f"  - Found {len(links)} links")

                for link, link_tag in links:
                    if link not in self.visited_urls and self.in_domain(link):
                        priority = self.priority(html_content, link, link_tag)
                        heapq.heappush(self.url_queue, (priority, link))

            except Exception as e:
                print("Error crawling:", e)

        driver.quit() 

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
        texts = [self.keyword, sourounding_text]
        word_vectors = vectorizer.fit_transform(texts)

        # Compute cosine similarity between the two bags of words
        similarity = cosine_similarity(word_vectors[0], word_vectors[1])[0][0]
        
        # compue priority based on vector similarity (more similar texts should result in higher priority (lower return number))
        priority = 1 - similarity
        return priority
    

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

    @staticmethod
    def compute_content_hash(html_content):
        return hashlib.md5(html_content.encode('utf-8')).hexdigest()

    # MinHash function to create signatures for sets
    @staticmethod
    def minhash(set_of_words, num_hashes=200):
        hashes = []
        for i in range(num_hashes):
            seed = random.randint(0, 2**32 - 1)
            min_hash_value = min(hash(f"{hash(seed)}:{word}") for word in set_of_words)
            hashes.append(min_hash_value)
        return hashes

    # Jaccard Similarity computation
    @staticmethod
    def jaccard_similarity(set1, set2):
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union

    # Function to detect duplicate based on Jaccard similarity
    def is_duplicate_using_jaccard(self, html_content, threshold=0.8):
        set_of_words = set(html_content.split())  # This will split by whitespace; more advanced tokenization can be used
        signature = self.minhash(set_of_words)

        # Compare against previously stored signatures
        for stored_signature in self.signatures:
            similarity = self.jaccard_similarity(set(signature), set(stored_signature))
            if similarity > threshold:
                return True
        
        self.signatures.append(signature)
        return False
    
if __name__ == "__main__":
    spider = SpiderMonkey(domain= "https://med.over.net/", max_workers=10, max_pages=5000)
    spider.start_crawler()

"""
    def set_visited_urls(self):
        Fetch all visited URLs and push the last one into a priority queue.
        self.visited_urls = db.get_all_urls()  # Fetch all URLs as a set

        if self.visited_urls:  # Ensure the set is not empty
            last_url = self.visited_urls.pop()  # Remove and get the last item
            heapq.heappush(self.url_queue, (0, last_url))  # Push it into the priority queue

            print(f"Pushed to queue: {last_url}")


"""