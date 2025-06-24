import openai
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def create_prompt(question_label: str, options_text: Optional[str] = None) -> str:
    """Creates the appropriate prompt based on whether options are provided"""
    if options_text:
        return f"""You are a helpful survey respondent. Please answer the following question.

Question: {question_label}

Available options: {options_text}

Please respond with ONLY one of the available options listed above. Do not include any additional text, explanations, or formatting. Just provide the exact text of your chosen option."""
    else:
        return f"""You are a helpful survey respondent. Please answer the following question.

Question: {question_label}

Please provide a clear response to this question. Be concise but thorough in your answer."""

def ask_question_with_llm(question_obj: Dict[str, Any], api_key: Optional[str] = None, model: str = "gpt-4o-mini") -> str:
    """
    Takes a question object and makes an OpenAI call to get an answer.
    
    Args:
        question_obj: Dictionary containing question information (key, type, label, options)
        api_key: OpenAI API key (if None, will try to get from OPENAI_API_KEY environment variable)
        model: OpenAI model to use (default: gpt-4o-mini)
    
    Returns:
        str: The answer from the LLM
    """
    # Get API key from parameter or environment variable
    if api_key is None:
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key is None:
            raise ValueError("OpenAI API key not provided and OPENAI_API_KEY environment variable not set")

    # Set up OpenAI client
    client = openai.OpenAI(api_key=api_key)
    
    # Extract question information
    question_type = question_obj.get('type', '')
    question_label = question_obj.get('label', '')
    options = question_obj.get('options', [])
    
    # Determine if this is a multiple choice question
    has_options = bool(options) and question_type in ['mc', 'matrix']
    
    # Create the prompt following HHH guidelines
    if has_options:
        # For multiple choice questions
        options_text = ", ".join(options)
        
    
    try:
        # Make the API call
        prompt = create_prompt(question_label, options_text if has_options else None)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful survey respondent who provides honest and thoughtful answers."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150 if has_options else 300,
            temperature=0.7
        )
        
        # Extract the answer
        answer = response.choices[0].message.content.strip()
        
        # For multiple choice questions, validate that the answer is one of the options
        if has_options and answer not in options:
            # If the answer doesn't match exactly, try to find the closest match
            answer_lower = answer.lower()
            for option in options:
                if option.lower() == answer_lower:
                    return option
            # If no match found, return the first option as fallback
            return options[0]
        
        return answer
        
    except Exception as e:
        print(f"Error making OpenAI API call: {e}")
        if has_options:
            return options[0] if options else "Error occurred"
        else:
            return "Error occurred while processing the question"


# Example usage:
# from convert_to_json import convert_yaml_to_json_objects
# 
# # Convert YAML to question objects
# questions = convert_yaml_to_json_objects('sample_q.yml')
# 
# # Ask a specific question (API key will be loaded from .env file)
# answer = ask_question_with_llm(questions[0])
# print(f"Question: {questions[0]['label']}")
# print(f"Answer: {answer}") 