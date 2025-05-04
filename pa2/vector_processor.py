from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer

class VectorProcessor:
    def __init__(self, model_name: str = 'sentence-transformers/LaBSE'):
        """Initialize the vector processor with SentenceTransformer model.
        
        Args:
            model_name: Name of the sentence-transformers model to use
                       Defaults to LaBSE which is good for multiple languages
        """
        self.model = SentenceTransformer(model_name)
        
    def create_embedding(self, text: str) -> List[float]:
        """Create an embedding vector for the given text.
        
        Args:
            text: The text to create an embedding for
            
        Returns:
            List of floats representing the embedding vector
            
        Raises:
            Exception: If embedding creation fails
        """
        try:
            # Convert the embedding to a regular list for database storage
            embedding = self.model.encode(text).tolist()
            return embedding
        except Exception as e:
            raise Exception(f"Failed to create embedding: {str(e)}")
            
    def process_segments(self, segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process multiple segments and add embeddings.
        
        Args:
            segments: List of dictionaries containing segment data
                     Each dict should have 'id' and 'text' keys
                     
        Returns:
            List of dictionaries with added 'embedding' key
        """
        processed_segments = []
        
        try:
            # Process all texts at once for better efficiency
            texts = [segment['text'] for segment in segments]
            embeddings = self.model.encode(texts)
            
            # Add embeddings back to segments
            for segment, embedding in zip(segments, embeddings):
                segment['embedding'] = embedding.tolist()
                processed_segments.append(segment)
                
        except Exception as e:
            print(f"Error processing segments batch: {str(e)}")
            
            # Fallback to processing one by one if batch fails
            for segment in segments:
                try:
                    embedding = self.create_embedding(segment['text'])
                    segment['embedding'] = embedding
                    processed_segments.append(segment)
                except Exception as e:
                    print(f"Error processing segment {segment['id']}: {str(e)}")
                
        return processed_segments

if __name__ == "__main__":
    # Example usage
    processor = VectorProcessor()
    test_segments = [
        {"id": 1, "text": "This is an example sentence"},
        {"id": 2, "text": "Each sentence is converted"}
    ]
    results = processor.process_segments(test_segments)
    print("Processed segments with embeddings:", results)
