# Vector Database Search System Documentation

## Overview

This document provides a comprehensive guide to the Vector Database Search System, a tool designed for semantic and hybrid text search using vector embeddings. The system leverages PostgreSQL with the pgvector extension to perform efficient vector similarity searches across stored document segments.

## System Architecture

The search system consists of the following components:

1. **Vector Database (PostgreSQL with pgvector)**: Stores document content and their vector embeddings
2. **VectorProcessor**: Generates vector embeddings for text using a pre-trained model
3. **VectorDBQuerier**: Provides methods to query the database using different search strategies
4. **HTMLCleaner**: Processes and cleans HTML content for storage (not directly used in querying)

## Database Schema

The system uses the following database structure:

- **Database**: VectorDB01
- **Schema**: crawldb
- **Tables**:
  - `cleaned_page`: Stores cleaned web pages
    - `id`: Primary key
    - `url`: URL of the page
    - `plain_text`: Full text content of the page
  - `page_segment`: Stores page segments with vector embeddings
    - `id`: Primary key
    - `page_id`: Foreign key referencing cleaned_page.id
    - `page_segment`: Text segment from the page
    - `embedding`: Vector embedding of the segment (using pgvector)

## Setup Requirements

### Software Dependencies

- Python 3.6+
- PostgreSQL with pgvector extension
- psycopg2 for PostgreSQL connection
- Vector embedding model (accessed via VectorProcessor)

### Environment Variables

The system uses the following environment variables, which can be set in a `.env` file:

- `DB_NAME`: Database name (default: "VectorDB01")
- `DB_USER`: Database user (default: "postgres")
- `DB_PASSWORD`: Database password (default: "Admin")
- `DB_HOST`: Database host (default: "localhost")
- `DB_PORT`: Database port (default: "5432")

## Search Capabilities

CHeckout UserExample.ipynb for preapred search examples

### 1. Semantic Search

Identifies documents with similar meaning to the query, regardless of exact keyword matches.

- **Method**: `semantic_search(query, limit, similarity_threshold)`
- **Algorithm**: Cosine similarity between query vector and stored vectors
- **Parameters**:
  - `query`: Text string to search for
  - `limit`: Maximum number of results (default: 5)
  - `similarity_threshold`: Minimum similarity score to include (default: 0.5)
- **Returns**: List of matching segments with metadata and similarity scores

### 2. Hybrid Search (Keyword + Semantic)

Combines traditional keyword matching with semantic similarity for more robust results.

- **Method**: `keyword_and_semantic_search(query, limit)`
- **Algorithm**: Weighted combination of full-text search and vector similarity
- **Parameters**:
  - `query`: Text string to search for
  - `limit`: Maximum number of results (default: 5)
- **Returns**: List of matching segments with metadata and combined scores

### 3. URL-based Content Search

Retrieves content from URLs matching a pattern, optionally filtered by a semantic query.

- **Method**: `url_content_search(url_pattern, query, limit)`
- **Parameters**:
  - `url_pattern`: Pattern to match URLs (SQL LIKE pattern)
  - `query`: Optional semantic query to filter results (default: None)
  - `limit`: Maximum number of results (default: 5)
- **Returns**: List of matching segments with metadata

### 4. Full Page Content Retrieval

Retrieves the complete content of a specific page by ID.

- **Method**: `get_page_content(page_id)`
- **Parameters**:
  - `page_id`: ID of the page to retrieve
- **Returns**: Tuple of (URL, full text content)

## Vector Similarity Implementation

The system uses the pgvector operator `<=>` to calculate cosine distance between vectors. The similarity score is calculated as:

```sql
1 - (embedding <=> query_embedding) AS similarity
```

This converts the distance measure to a similarity score, where:
- 1.0 represents perfect similarity (identical vectors)
- 0.0 represents complete dissimilarity (orthogonal vectors)

## Command Line Interface

The system provides a command-line interface with the following options:

```
python query.py [OPTIONS]
```

### Options:

- `--query`, `-q`: Semantic search query
- `--url`, `-u`: URL pattern to search within
- `--hybrid`, `-y`: Use hybrid search (semantic + keyword)
- `--limit`, `-l`: Maximum number of results (default: 5)
- `--page`, `-p`: Get full content of specific page ID
- `--threshold`, `-t`: Similarity threshold (0-1) (default: 0.5)

### Example Usage:

```bash
# Semantic search
python Vector_db_querier.py -q "Erasmus"

# Hybrid search
python Vector_db_querier.py -q "Erasmus" -y

# URL pattern search
python Vector_db_querier.py -u "Erasmus"

# Combined URL and query search
python Vector_db_querier.py -u "Erasmus" -q "korea"

# Retrieve full page content
python Vector_db_querier.py -p 123
```

## Implementation Details

### Vector Search Process

1. The query text is converted to a vector embedding using the VectorProcessor
2. The embedding is used in a PostgreSQL query to find similar content
3. Results are sorted by similarity and filtered by threshold
4. Metadata and content are returned to the user

### Hybrid Search Algorithm

Hybrid search combines two ranking signals:
1. Text rank from PostgreSQL's full-text search (40% weight)
2. Vector similarity from pgvector (60% weight)

The combined score is calculated as:
```sql
(ts_rank_cd(to_tsvector('english', ps.page_segment), plainto_tsquery('english', %s)) * 0.4 + 
 (1 - (ps.embedding <=> %s::float[])) * 0.6) AS combined_score
```

## Error Handling

The system implements try-except blocks to handle various error scenarios:
- Database connection issues
- Query execution errors
- Embedding generation failures

Error messages are printed to the console, and empty result lists are returned in case of errors.

## Performance Considerations

For optimal performance:
1. Ensure the PostgreSQL database has appropriate indexes on the embedding column
2. Use a reasonable similarity threshold to filter out irrelevant results
3. Limit result sizes for faster response times

## Best Practices

1. **Query Construction**: Keep queries concise and focused for better semantic matching
2. **Similarity Threshold**: Start with a threshold of 0.5 and adjust based on result quality
3. **URL Patterns**: Use specific URL patterns to narrow search scope when possible
4. **Hybrid Search**: Use for queries where both semantic and keyword relevance matter

## Conclusion

This Vector Database Search System provides powerful semantic and hybrid search capabilities for document collections. By leveraging vector embeddings and PostgreSQL's pgvector extension, it enables finding content based on meaning rather than just keywords, significantly improving search quality for complex queries.