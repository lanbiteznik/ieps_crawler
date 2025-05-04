import os
import dotenv
import argparse
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from vector_processor import VectorProcessor
from html_cleaner import HTMLCleaner


dotenv.load_dotenv(override=True)
db_name = os.getenv("DB_NAME", "VectorDB01")
db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "Admin")
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "5432")


class VectorDBQuerier:
    def __init__(self, vector_processor: VectorProcessor):
        """Initialize the vector database querier.
        
        Args:
            vector_processor: Instance of VectorProcessor for creating embeddings
        """
        
        self.vector_processor = vector_processor
        self.connection = self._get_connection()
        print(f"Connecting to the database {db_name}")
        
    def _get_connection(self):
        """Establish database connection."""
        connection_params = {
            "host": db_host,
            "port": db_port,
            "database": db_name,
            "user": db_user,
            "password": db_password,
            "cursor_factory": RealDictCursor
        }
        return psycopg2.connect(**connection_params)
    
    def semantic_search(self, query: str, limit: int = 5, similarity_threshold: float = 0.5) -> List[Dict[str, Any]]:
        """Search for semantically similar content based on vector similarity.
        
        Args:
            query: The search query text
            limit: Maximum number of results to return
            similarity_threshold: Minimum similarity score (0-1) to include in results
            
        Returns:
            List of matching segments with their metadata and similarity scores
        """
        try:
            query_embedding = self.vector_processor.create_embedding(query)
            
            cursor = self.connection.cursor()
            
            cursor.execute("""
    SELECT 
        ps.id AS segment_id,
        ps.page_id,
        ps.page_segment,
        cp.url,
        1 - (ps.embedding <=> %s::vector) AS similarity
    FROM 
        crawldb.page_segment ps
    JOIN 
        crawldb.cleaned_page cp ON ps.page_id = cp.id
    WHERE 
        1 - (ps.embedding <=> %s::vector) > %s
    ORDER BY 
        similarity DESC
    LIMIT %s;
""", (query_embedding, query_embedding, similarity_threshold, limit))

            
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except Exception as e:
            print(f"Error in semantic search: {str(e)}")
            return []
    
    def keyword_and_semantic_search(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Hybrid search combining keyword matching and semantic similarity."""
        try:
            query_embedding = self.vector_processor.create_embedding(query)
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT 
                    ps.id AS segment_id,
                    ps.page_id,
                    ps.page_segment,
                    cp.url,
                    ts_rank_cd(to_tsvector('english', ps.page_segment), plainto_tsquery('english', %s)) AS text_rank,
                    1 - (ps.embedding <=> %s::vector) AS vector_similarity,
                    (ts_rank_cd(to_tsvector('english', ps.page_segment), plainto_tsquery('english', %s)) * 0.4 + 
                    (1 - (ps.embedding <=> %s::vector)) * 0.6) AS combined_score
                FROM 
                    crawldb.page_segment ps
                JOIN 
                    crawldb.cleaned_page cp ON ps.page_id = cp.id
                WHERE 
                    to_tsvector('english', ps.page_segment) @@ plainto_tsquery('english', %s)
                    OR 1 - (ps.embedding <=> %s::vector) > 0.6
                ORDER BY 
                    combined_score DESC
                LIMIT %s;
            """, (query, query_embedding, query, query_embedding, query, query_embedding, limit))

            results = cursor.fetchall()
            cursor.close()
            return results
            
        except Exception as e:
            print(f"Error in hybrid search: {str(e)}")
            return []
    
    def url_content_search(self, url_pattern: str, query: str = None, limit: int = 5) -> List[Dict[str, Any]]:
        """Search for content within URLs matching a pattern, optionally filtered by query.
        
        Args:
            url_pattern: Pattern to match URLs (SQL LIKE pattern)
            query: Optional semantic query to filter results
            limit: Maximum number of results to return
            
        Returns:
            List of matching segments with their metadata
        """
        try:
            cursor = self.connection.cursor()
            
            if query:
                query_embedding = self.vector_processor.create_embedding(query)
                
                cursor.execute("""
                    SELECT 
                        ps.id AS segment_id,
                        ps.page_id,
                        ps.page_segment,
                        cp.url,
                        1 - (ps.embedding <=> %s::float[]) AS similarity
                    FROM 
                        crawldb.page_segment ps
                    JOIN 
                        crawldb.cleaned_page cp ON ps.page_id = cp.id
                    WHERE 
                        cp.url LIKE %s
                    ORDER BY 
                        similarity DESC
                    LIMIT %s;
                """, (query_embedding, f"%{url_pattern}%", limit))
            else:
                cursor.execute("""
                    SELECT 
                        ps.id AS segment_id,
                        ps.page_id,
                        ps.page_segment,
                        cp.url
                    FROM 
                        crawldb.page_segment ps
                    JOIN 
                        crawldb.cleaned_page cp ON ps.page_id = cp.id
                    WHERE 
                        cp.url LIKE %s
                    LIMIT %s;
                """, (f"%{url_pattern}%", limit))
            
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except Exception as e:
            print(f"Error in URL search: {str(e)}")
            return []
    
    def get_page_content(self, page_id: int) -> Tuple[str, str]:
        """Retrieve the full content of a specific page.
        
        Args:
            page_id: ID of the page to retrieve
            
        Returns:
            Tuple of (URL, full text content)
        """
        try:
            cursor = self.connection.cursor()
            
            cursor.execute("""
                SELECT url, plain_text
                FROM crawldb.cleaned_page
                WHERE id = %s;
            """, (page_id,))
            
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return result['url'], result['plain_text']
            else:
                return None, None
                
        except Exception as e:
            print(f"Error retrieving page content: {str(e)}")
            return None, None
    
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()


def display_results(results: List[Dict[str, Any]], show_similarity: bool = True):
    """Pretty print search results.
    
    Args:
        results: List of search results
        show_similarity: Whether to display similarity scores
    """
    if not results:
        print("No results found.")
        return
        
    print(f"\n=== Found {len(results)} results ===\n")
    
    for i, result in enumerate(results, 1):
        print(f"Result #{i}:")
        print(f"URL: {result['url']}")
        print(f"Page ID: {result['page_id']}")
        print(f"Segment ID: {result['segment_id']}")
        
        if show_similarity and 'similarity' in result:
            print(f"Similarity: {result['similarity']:.4f}")
        if 'text_rank' in result:
            print(f"Text Rank: {result['text_rank']:.4f}")
        if 'combined_score' in result:
            print(f"Combined Score: {result['combined_score']:.4f}")
            
        text = result['page_segment']
        if len(text) > 300:
            text = text[:297] + "..."
            
        print(f"Content: {text}")
        print("-" * 80)


def main():
    parser = argparse.ArgumentParser(description="Query the vector database")
    parser.add_argument("--query", "-q", type=str, help="Semantic search query")
    parser.add_argument("--url", "-u", type=str, help="URL pattern to search within")
    parser.add_argument("--hybrid", "-y", action="store_true", help="Use hybrid search (semantic + keyword)")
    parser.add_argument("--limit", "-l", type=int, default=5, help="Maximum number of results")
    parser.add_argument("--page", "-p", type=int, help="Get full content of specific page ID")
    parser.add_argument("--threshold", "-t", type=float, default=0.5, help="Similarity threshold (0-1)")
    
    args = parser.parse_args()
    
    args.query = "erazmus+"
    #args.url = "https://www.fri.uni-lj.si/sl/raziskave/raziskovalni-projekti"
    args.hybrid = True
    
    vector_processor = VectorProcessor()
    querier = VectorDBQuerier(vector_processor)
    
    try:
        if args.page is not None:
            url, content = querier.get_page_content(args.page)
            if url and content:
                print(f"\n=== Content for page {args.page} ===")
                print(f"URL: {url}")
                print("\nContent:")
                print("=" * 80)
                print(content)
                print("=" * 80)
            else:
                print(f"No page found with ID {args.page}")
                
        elif args.query and args.url:
            # Search within specific URL pattern using query
            print(f"Searching for '{args.query}' within URLs matching '{args.url}'...")
            results = querier.url_content_search(args.url, args.query, args.limit)
            display_results(results)
            
        elif args.url:
            # Search for content in URLs matching pattern
            print(f"Searching for content in URLs matching '{args.url}'...")
            results = querier.url_content_search(args.url, limit=args.limit)
            display_results(results, show_similarity=False)
            
        elif args.hybrid and args.query:
            # Hybrid search
            print(f"Performing hybrid search for '{args.query}'...")
            results = querier.keyword_and_semantic_search(args.query, args.limit)
            display_results(results)
            
        elif args.query:
            # Regular semantic search
            print(f"Searching for '{args.query}'...")
            results = querier.semantic_search(args.query, args.limit, args.threshold)
            display_results(results)
            
        else:
            # Show examples if no arguments provided
            print("No search parameters provided. Here are some examples:")
            print("\nSemantic search:")
            print("  python query.py -q 'renewable energy projects'")
            print("\nHybrid search:")
            print("  python query.py -q 'climate change policies' -y")
            print("\nURL pattern search:")
            print("  python query.py -u 'gov.si'")
            print("\nCombined URL and query search:")
            print("  python query.py -u 'gov.si' -q 'environmental regulations'")
            print("\nRetrieve full page content:")
            print("  python query.py -p 123")
            
    finally:
        querier.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSearch terminated by user.")
    except Exception as e:
        print(f"Error: {str(e)}")