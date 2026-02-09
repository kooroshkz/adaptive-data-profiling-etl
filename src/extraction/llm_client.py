# send chunks to llm for extraction
import os
from dotenv import load_dotenv

load_dotenv()


def call_llm(prompt, text_chunk, system_prompt=None):
    """send text to llm and get structured response"""
    provider = os.getenv('LLM_PROVIDER', 'openai')
    
    if provider == 'openai':
        return call_openai(prompt, text_chunk, system_prompt)
    elif provider == 'anthropic':
        return call_anthropic(prompt, text_chunk, system_prompt)
    else:
        raise ValueError(f"unknown provider: {provider}")


def call_openai(prompt, text_chunk, system_prompt):
    """openai api call"""
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    
    messages.append({
        "role": "user", 
        "content": f"{prompt}\n\n---\n{text_chunk}"
    })
    
    response = client.chat.completions.create(
        model=os.getenv('MODEL_NAME', 'gpt-4-turbo-preview'),
        messages=messages,
        temperature=0.1
    )
    
    return response.choices[0].message.content


def call_anthropic(prompt, text_chunk, system_prompt):
    """anthropic api call"""
    from anthropic import Anthropic
    client = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
    
    message = client.messages.create(
        model=os.getenv('MODEL_NAME', 'claude-3-sonnet-20240229'),
        max_tokens=1024,
        system=system_prompt if system_prompt else "",
        messages=[{
            "role": "user",
            "content": f"{prompt}\n\n---\n{text_chunk}"
        }],
        temperature=0.1
    )
    
    return message.content[0].text
