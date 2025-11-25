"""
FastAPI Integration for Synthea Neo4j Chatbot
==============================================
This module provides a REST API for querying the Synthea healthcare database
using natural language through Google Gemini.

Usage:
    uvicorn synthea_api:app --reload --port 8000

Endpoints:
    POST /ask - Ask a natural language question
    GET /stats - Get database statistics
    GET /samples - Get sample patients
    GET /schema - Get database schema
    GET /health - Health check
"""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Any
import uvicorn
import json

from synthea_chatbot_gemini import SyntheaChatbot

# Initialize FastAPI app
app = FastAPI(
    title="Healthcare Chatbot API",
    description="Query Synthea Neo4j database using natural language powered by Google Gemini",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize chatbot (singleton)
chatbot: Optional[SyntheaChatbot] = None


def get_chatbot() -> SyntheaChatbot:
    """Get or create chatbot instance"""
    global chatbot
    if chatbot is None:
        chatbot = SyntheaChatbot()
    return chatbot


# Request/Response models
class QuestionRequest(BaseModel):
    question: str

    class Config:
        json_schema_extra = {
            "example": {
                "question": "How many patients are in the database?"
            }
        }


class AnswerResponse(BaseModel):
    answer: str
    cypher_query: Optional[str] = None
    raw_results: Optional[Any] = None
    success: bool = True
    error: Optional[str] = None


class StatsResponse(BaseModel):
    stats: list
    success: bool = True


class SamplesResponse(BaseModel):
    patients: list
    success: bool = True


class SchemaResponse(BaseModel):
    schema_info: str
    success: bool = True


class HealthResponse(BaseModel):
    status: str
    database_connected: bool
    llm_connected: bool


# Endpoints
@app.on_event("startup")
async def startup_event():
    """Initialize chatbot on startup"""
    try:
        get_chatbot()
        print("Chatbot initialized successfully on startup")
    except Exception as e:
        print(f"Warning: Failed to initialize chatbot on startup: {e}")


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Synthea Healthcare Chatbot API",
        "version": "1.0.0",
        "endpoints": {
            "POST /ask": "Ask a natural language question",
            "WS /ws/ask": "WebSocket for real-time question answering",
            "GET /stats": "Get database statistics",
            "GET /samples": "Get sample patients",
            "GET /schema": "Get database schema",
            "GET /health": "Health check"
        },
        "example": {
            "endpoint": "POST /ask",
            "body": {"question": "How many patients have diabetes?"}
        },
        "websocket_example": {
            "endpoint": "ws://localhost:8000/ws/ask",
            "send": {"question": "How many patients have diabetes?"},
            "receive": {
                "type": "answer",
                "answer": "...",
                "cypher_query": "...",
                "success": True
            }
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Check API and database health"""
    try:
        bot = get_chatbot()
        # Test database connection
        bot.graph.query("RETURN 1")
        return HealthResponse(
            status="healthy",
            database_connected=True,
            llm_connected=True
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            database_connected=False,
            llm_connected=False
        )


@app.post("/ask", response_model=AnswerResponse, tags=["Query"])
async def ask_question(request: QuestionRequest):
    """
    Ask a natural language question about the healthcare data.

    Example questions:
    - How many patients are in the database?
    - Show me patients with diabetes
    - What are the most common conditions?
    - Which medications are prescribed most often?
    """
    try:
        bot = get_chatbot()
        response = bot.ask(request.question, max_retries=2)

        return AnswerResponse(
            answer=response['answer'],
            cypher_query=response.get('cypher_query'),
            raw_results=response.get('raw_results'),
            success=True
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats", response_model=StatsResponse, tags=["Database"])
async def get_stats():
    """Get database statistics showing count of each node type"""
    try:
        bot = get_chatbot()
        stats = bot.get_database_stats()
        return StatsResponse(stats=stats, success=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/samples", response_model=SamplesResponse, tags=["Database"])
async def get_samples(limit: int = 5):
    """Get sample patient records"""
    try:
        bot = get_chatbot()
        samples = bot.get_sample_patients(limit=limit)
        return SamplesResponse(patients=samples, success=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/schema", response_model=SchemaResponse, tags=["Database"])
async def get_schema():
    """Get the Neo4j database schema"""
    try:
        bot = get_chatbot()
        return SchemaResponse(schema_info=bot.schema, success=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/ask")
async def websocket_ask(websocket: WebSocket):
    """
    WebSocket endpoint for real-time natural language question answering.

    Usage:
        Connect to ws://localhost:8000/ws/ask
        Send JSON: {"question": "How many patients have diabetes?"}
        Receive JSON responses with answer, query, and results

    Example with JavaScript:
        const ws = new WebSocket('ws://localhost:8000/ws/ask');
        ws.onopen = () => {
            ws.send(JSON.stringify({question: "How many patients are there?"}));
        };
        ws.onmessage = (event) => {
            const response = JSON.parse(event.data);
            console.log(response.answer);
        };
    """
    await websocket.accept()

    try:
        # Send welcome message
        await websocket.send_json({
            "type": "connected",
            "message": "Connected to Synthea Healthcare Chatbot",
            "status": "ready"
        })

        bot = get_chatbot()

        while True:
            # Receive message from client
            data = await websocket.receive_text()

            try:
                # Parse incoming message
                message = json.loads(data)
                question = message.get("question", "").strip()

                if not question:
                    await websocket.send_json({
                        "type": "error",
                        "error": "Question cannot be empty",
                        "success": False
                    })
                    continue

                # Send processing status
                await websocket.send_json({
                    "type": "processing",
                    "message": "Processing your question...",
                    "question": question
                })

                # Get answer from chatbot
                response = bot.ask(question, max_retries=2)

                # Send successful response
                await websocket.send_json({
                    "type": "answer",
                    "question": question,
                    "answer": response['answer'],
                    "cypher_query": response.get('cypher_query'),
                    "raw_results": response.get('raw_results'),
                    "success": True
                })

            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "error": "Invalid JSON format. Please send: {\"question\": \"your question\"}",
                    "success": False
                })
            except Exception as e:
                await websocket.send_json({
                    "type": "error",
                    "error": str(e),
                    "success": False
                })

    except WebSocketDisconnect:
        print("WebSocket client disconnected")
    except Exception as e:
        print(f"WebSocket error: {e}")
        try:
            await websocket.send_json({
                "type": "error",
                "error": f"Server error: {str(e)}",
                "success": False
            })
        except:
            pass


if __name__ == "__main__":
    uvicorn.run(
        "synthea_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
