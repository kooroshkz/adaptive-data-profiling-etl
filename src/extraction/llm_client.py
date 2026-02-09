# llm extraction with openai
import os
import json
from dotenv import load_dotenv

load_dotenv()


def extract_with_gpt(prompt, text_chunk, response_format="json"):
    """extract structured data using gpt-4"""
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    
    messages = [
        {
            "role": "system",
            "content": "You are a precise data extraction assistant. Extract ESG metrics from sustainability reports. Return valid JSON only."
        },
        {
            "role": "user",
            "content": f"{prompt}\n\n---TEXT---\n{text_chunk}\n---END TEXT---"
        }
    ]
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",  # faster and cheaper for extraction
        messages=messages,
        temperature=0,
        response_format={"type": "json_object"} if response_format == "json" else None
    )
    
    return response.choices[0].message.content


def search_relevant_pages(data, keywords, max_pages=15):
    """find pages containing keywords - expanded search"""
    relevant = []
    scored_pages = []
    
    for page in data['pages']:
        text_lower = page['text'].lower()
        score = 0
        
        # score pages by keyword matches
        for keyword in keywords:
            count = text_lower.count(keyword.lower())
            score += count
        
        if score > 0:
            scored_pages.append((score, page))
    
    # sort by score and take top pages
    scored_pages.sort(reverse=True, key=lambda x: x[0])
    relevant = [page for _, page in scored_pages[:max_pages]]
    
    return relevant


def prepare_extraction_context(pages, max_chars=12000):
    """combine relevant pages into extraction context - increased limit"""
    context = ""
    
    for page in pages:
        page_text = f"=== Page {page['page']} ===\n{page['text']}\n\n"
        
        if len(context) + len(page_text) > max_chars:
            break
        
        context += page_text
    
    return context
