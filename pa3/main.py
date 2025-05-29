# main.py

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn
from pathlib import Path
import os
import sys
import dotenv
from typing import List, Dict, Any
import openai

# Add parent directory to path to import from PA2
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
pa2_dir = os.path.join(parent_dir, "pa2", "implementation-extraction")
sys.path.append(pa2_dir)

# Import from PA2
from vector_processor import VectorProcessor
from Vector_db_querier import VectorDBQuerier

# Load environment variables
dotenv.load_dotenv(override=True)
openai.api_key = os.getenv("OPENAI_API_KEY")

app = FastAPI()

class QuestionRequest(BaseModel):
    question: str
    use_rag: bool

class AnswerResponse(BaseModel):
    answer: str

# Initialize vector search components
vector_processor = None
vector_db_querier = None

def initialize_vector_search():
    global vector_processor, vector_db_querier
    try:
        vector_processor = VectorProcessor()
        vector_db_querier = VectorDBQuerier(vector_processor)
        return True
    except Exception as e:
        print(f"Error initializing vector search: {str(e)}")
        return False

def get_relevant_documents(query: str, limit: int = 3) -> List[Dict[str, Any]]:
    """Retrieve relevant documents for the query."""
    try:
        if not vector_db_querier:
            print("Vector DB querier not initialized")
            return []
        
        results = vector_db_querier.keyword_and_semantic_search(query, limit=limit)
        # Log successful retrieval
        print(f"Retrieved {len(results)} documents for query: '{query}'")
        return results
    except Exception as e:
        print(f"Error retrieving documents: {str(e)}")
        return []

def format_context_from_documents(docs: List[Dict[str, Any]]) -> str:
    """Format retrieved documents into context for the LLM."""
    if not docs:
        return ""
    
    context_parts = []
    for i, doc in enumerate(docs, 1):
        url = doc.get('url', 'Unknown URL')
        text = doc.get('page_segment', '')
        context_parts.append(f"Document {i} (Source: {url}):\n{text}\n")
    
    return "\n".join(context_parts)

def generate_answer_with_llm(question: str, context: str = None) -> str:
    """Generate an answer using the LLM."""
    try:
        if context:
            # For RAG mode: include context in prompt
            prompt = f"""Please answer this question based ONLY on the provided context documents.
            If the answer cannot be found in the context, say "I don't have enough information to answer that question."

            CONTEXT:
            {context}

            QUESTION: {question}
            """
        else:
            # For direct mode: just the question
            prompt = f"Please answer this question: {question}"
        
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5,
            max_tokens=500
        )
        
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error generating answer: {str(e)}")
        return f"Sorry, I encountered an error while generating an answer: {str(e)}"

@app.post("/question", response_model=AnswerResponse)
async def answer_question(request: QuestionRequest):
    if not vector_db_querier and not initialize_vector_search():
        return AnswerResponse(answer="Error: Vector search system is not available")
    
    try:
        if request.use_rag:
            # RAG-based: retrieve documents and format them
            relevant_docs = get_relevant_documents(request.question)
            if not relevant_docs:
                return AnswerResponse(answer="No relevant documents found for your question.")
            
            # Format documents into readable form
            formatted_docs = []
            for i, doc in enumerate(relevant_docs, 1):
                url = doc.get('url', 'Unknown URL')
                text = doc.get('page_segment', '')
                similarity = doc.get('similarity', 0)
                formatted_docs.append(f"Document {i} (Relevance: {similarity:.2f}):\n{text}\n\nSource: {url}\n")
            
            answer = "Retrieved the following documents:\n\n" + "\n".join(formatted_docs)
            return AnswerResponse(answer=answer)
        else:
            # Simple mode (no retrieval)
            return AnswerResponse(answer="Direct question answering not implemented yet. Please use RAG mode.")
    except Exception as e:
        return AnswerResponse(answer=f"Error: {str(e)}")

@app.get("/", response_class=HTMLResponse)
async def get_form():
    html_path = Path("templates/index.html")
    return HTMLResponse(content=html_path.read_text(), status_code=200)

# Add this to test database connection
@app.get("/test-db-connection")
async def test_db_connection():
    if initialize_vector_search():
        return {"status": "success", "message": "Database connection successful"}
    return {"status": "error", "message": "Failed to connect to database"}

# Add this to test document retrieval
@app.get("/test-retrieval")
async def test_retrieval(query: str, limit: int = 3):
    if not vector_db_querier and not initialize_vector_search():
        return {"error": "Vector search system is not available"}
    
    try:
        docs = get_relevant_documents(query, limit)
        context = format_context_from_documents(docs)
        return {
            "query": query,
            "document_count": len(docs),
            "documents": docs,
            "formatted_context": context
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    initialize_vector_search()
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

