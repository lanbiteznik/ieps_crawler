# main.py
import sys
import os
from pathlib import Path
# Adds the project root (ieps_crawler) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn
from pathlib import Path
import os
import sys
import dotenv
from typing import List, Dict, Any
import requests  
import time
from datetime import datetime
import re  # Add this import at the top of your file

# Add parent directory to path to import from PA2
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
pa2_dir = os.path.join(parent_dir, "pa2", "implementation-extraction")
sys.path.append(pa2_dir)

# Import from PA2
from pa2.implementation_extraction.vector_processor import VectorProcessor
from pa2.implementation_extraction.Vector_db_querier import VectorDBQuerier


# Load environment variables
dotenv.load_dotenv(override=True)
# No need for OpenAI API key, using local Ollama

# Ollama settings
OLLAMA_API_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "deepseek-r1:latest"

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

def get_relevant_documents(query: str, limit: int = 7) -> List[Dict[str, Any]]:
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
    """Generate an answer using Ollama with deepseek-r1 model."""
    start_time = time.time()
    try:
        if context:
            # For RAG mode: include context in prompt with clear instructions
            prompt = f"""Answer the following question.
            Provide a direct, concise answer without showing your reasoning process but still giving an explenation.
            Do not include phrases like "Based on the context" or "According to the documents."
            If the answer cannot be found in the context do your best to provide a reasonable answer based on your knowledge.
            If the question is not in english, check if it could be in slovene and answer in slovene.
            If the question is not in either language, answer it in the language that it was asked in.
            Never include any content inside <think> tags.

            CONTEXT:
            {context}

            QUESTION: {question}
            
            ANSWER:"""
        else:
            # For direct mode: just the question with improved instructions
            prompt = f"""Please answer and explain this question directly and concisely:

            QUESTION: {question}
            
            ANSWER:"""
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sending request to LLM...")
        # Call Ollama API
        payload = {
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "temperature": 0.5,
            "max_tokens": 500
        }
        
        response = requests.post(OLLAMA_API_URL, json=payload)
        
        if response.status_code == 200:
            answer = response.json()["response"].strip()
            
            # Remove any content between <think> and </think> tags
            answer = re.sub(r'<think>.*?</think>', '', answer, flags=re.DOTALL)
            
            # Clean up the answer if it still has formatting issues
            if answer.lower().startswith("answer:"):
                answer = answer[7:].strip()
            
            llm_time = time.time() - start_time
            print(f"[{datetime.now().strftime('%H:%M:%S')}] LLM response received in {llm_time:.2f} seconds")
            return answer
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ollama API error: {response.status_code}")
            return f"Error: Failed to get response from Ollama (Status code: {response.status_code})"
            
    except Exception as e:
        llm_time = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Error generating answer after {llm_time:.2f} seconds: {str(e)}")
        return f"Sorry, I encountered an error while generating an answer: {str(e)}"

@app.post("/question", response_model=AnswerResponse)
async def answer_question(request: QuestionRequest):
    start_time = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing query: '{request.question}', RAG mode: {request.use_rag}")
    
    if not vector_db_querier and not initialize_vector_search():
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Error: Vector search system unavailable")
        return AnswerResponse(answer="Error: Vector search system is not available")
    
    try:
        if request.use_rag:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Retrieving relevant documents...")
            # RAG-based answer: retrieve documents and use them as context
            relevant_docs = get_relevant_documents(request.question)
            
            if not relevant_docs:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] No documents found")
                return AnswerResponse(answer="No relevant documents found for your question.")
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(relevant_docs)} documents, formatting context...")
            # Format the context from documents
            context = format_context_from_documents(relevant_docs)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Generating answer with context...")
            # Generate answer with context using the LLM
            answer = generate_answer_with_llm(request.question, context)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Direct query to LLM without context...")
            # Simple answer: direct LLM without context
            answer = generate_answer_with_llm(request.question)
        
        elapsed_time = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Query completed in {elapsed_time:.2f} seconds")
        
        # Include processing time in the answer
        answer_with_time = f"{answer}\n\n[Query processed in {elapsed_time:.2f} seconds]"
        return AnswerResponse(answer=answer_with_time)
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Error after {elapsed_time:.2f} seconds: {str(e)}")
        return AnswerResponse(answer=f"Error: {str(e)}")

@app.get("/", response_class=HTMLResponse)
async def get_form():
    BASE_DIR = Path(__file__).resolve().parent
    html_path = BASE_DIR / "templates" / "index.html"
    #html_path = Path("templates/index.html")
    return HTMLResponse(content=html_path.read_text(), status_code=200)

# Add this to test database connection
@app.get("/test-db-connection")
async def test_db_connection():
    if initialize_vector_search():
        return {"status": "success", "message": "Database connection successful"}
    return {"status": "error", "message": "Failed to connect to database"}

# Add this to test document retrieval
@app.get("/test-retrieval")
async def test_retrieval(query: str, limit: int = 7):
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

# Add this to test Ollama connection
@app.get("/test-ollama")
async def test_ollama():
    try:
        response = requests.post(OLLAMA_API_URL, json={
            "model": OLLAMA_MODEL,
            "prompt": "Say hello",
            "stream": False
        })
        
        if response.status_code == 200:
            return {"status": "success", "response": response.json()["response"]}
        else:
            return {"status": "error", "message": f"Ollama error: {response.status_code}, {response.text}"}
    except Exception as e:
        return {"status": "error", "message": f"Error connecting to Ollama: {str(e)}"}

if __name__ == "__main__":
    initialize_vector_search()
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

