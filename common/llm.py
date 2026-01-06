import os

from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv

load_dotenv()

lama_model=ChatGroq(
    model = "llama-3.3-70b-versatile",
    api_key=os.getenv("GROQ_API_KEY")
    )

gemini_model=ChatOpenAI(
    model="google/gemini-2.5-flash-lite",
    api_key=os.getenv("OPEN_ROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1",
    )
