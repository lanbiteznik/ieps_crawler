# README: Estrella Web Crawler

## Overview
Estrella is a multi-threaded web crawler designed to scrape web pages, extract textual and binary data, and store them in a PostgreSQL database. It employs various techniques such as sitemap parsing, robots.txt compliance, MinHash-based similarity detection, and multi-threaded crawling.

## Features
- **Multi-threaded crawling** for efficiency
- **Sitemap extraction** to optimize URL discovery
- **Robots.txt parsing** to ensure compliance with site policies
- **Duplicate detection** using MinHash signatures
- **Binary file extraction** (PDFs, images, etc.)
- **Integration with PostgreSQL** for storing crawled data
- **Uses Selenium for dynamic content rendering**
- **Keyword-based filtering** for focused crawling

## Dependencies
Ensure the following Python libraries are installed:
```sh
pip install -r requirements.txt
```

## Environment Variables
Create a `.env` file to store database credentials:
```
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=your_port
```

## Database Setup
The crawler requires a PostgreSQL database. The `PostgresDB` class handles:
1. **Connecting to the database**
2. **Schema initialization**
3. **Storing site data, pages, and extracted content**

## Usage
To start crawling a domain, initialize and run Estrella:
```python
crawler = Estrella("https://example.com")
crawler.crawl()
```

## Key Components
### 1. **SitemapFetcher**
Finds sitemaps from robots.txt or common paths and extracts URLs.

### 2. **MinHash**
Detects duplicate pages by computing MinHash signatures of tokenized text.

### 3. **Estrella (Main Crawler Class)**
- Initializes database connection
- Parses robots.txt and sitemaps
- Uses a priority queue for efficient crawling
- Detects and classifies page content
- Extracts and stores binary files (PDFs, DOCs, etc.)

## Content Classification
- **HTML Pages**: Extracted using Selenium or requests
- **Binary Files**: Identified by MIME types and stored
- **Duplicate Detection**: Uses MinHash to prevent redundant storage

## Extending/using the Crawler
You can modify the following to customize functionality:
- `self.keywords` to focus on specific topics
- `extract_binary_files_from_html()` to add new file types
- `MinHash` to refine duplicate detection criteria


## License
This project is released under the MIT License.

