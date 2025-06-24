import psycopg2
import os
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables
load_dotenv()


class SupabaseHandler:
    """Handler for Supabase database operations"""
    
    def __init__(self):
        """Initialize database connection parameters from environment variables"""
        self.host = os.getenv('SUPABASE_HOST')
        self.port = os.getenv('SUPABASE_PORT', '6543')
        self.database = os.getenv('SUPABASE_DATABASE', 'postgres')
        self.user = os.getenv('SUPABASE_USER')
        self.password = os.getenv('SUPABASE_PASSWORD')
        
        # Validate required environment variables
        if not all([self.host, self.user, self.password]):
            raise ValueError("Missing required Supabase environment variables. Please check your .env file.")
    
    def get_connection(self):
        """Create and return a database connection"""
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            return connection
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def read_table(self, table_name: str, columns: str = "*", where_clause: str = None, limit: int = None) -> List[Dict[str, Any]]:
        """
        Read data from any table in the database
        
        Args:
            table_name: Name of the table to read from
            columns: Columns to select (default: "*")
            where_clause: Optional WHERE clause (without the WHERE keyword)
            limit: Optional limit on number of rows
            
        Returns:
            List of dictionaries containing the table data
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            # Build the query
            query = f"SELECT {columns} FROM {table_name}"
            params = []
            
            if where_clause:
                query += f" WHERE {where_clause}"
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, params)
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Fetch all rows and convert to list of dictionaries
            rows = cursor.fetchall()
            result = []
            
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    # Handle different data types
                    if isinstance(value, datetime):
                        row_dict[column_names[i]] = value.isoformat()
                    elif isinstance(value, (dict, list)):
                        row_dict[column_names[i]] = json.dumps(value) if value else None
                    else:
                        row_dict[column_names[i]] = value
                result.append(row_dict)
            
            print(f"Successfully read {len(result)} rows from table '{table_name}'")
            return result
            
        except Exception as e:
            print(f"Error reading from table '{table_name}': {e}")
            return []
        finally:
            cursor.close()
            connection.close()
    
    def read_star_wars_test1(self) -> List[Dict[str, Any]]:
        """
        Specifically read from the StarWarsTest1 table
        
        Returns:
            List of dictionaries containing the StarWarsTest1 data
        """
        return self.read_table('StarWarsTest1')
    
    def create_tables_if_not_exist(self):
        """Create necessary tables if they don't exist"""
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            # Create survey_responses table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS survey_responses (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(255) NOT NULL,
                    question_key VARCHAR(255) NOT NULL,
                    question_label TEXT NOT NULL,
                    question_type VARCHAR(50) NOT NULL,
                    answer TEXT NOT NULL,
                    options JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create survey_sessions table to track complete survey sessions
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS survey_sessions (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(255) UNIQUE NOT NULL,
                    total_questions INTEGER DEFAULT 0,
                    completed_questions INTEGER DEFAULT 0,
                    status VARCHAR(50) DEFAULT 'in_progress',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                )
            """)
            
            connection.commit()
            print("Database tables created/verified successfully")
            
        except Exception as e:
            connection.rollback()
            print(f"Error creating tables: {e}")
            raise
        finally:
            cursor.close()
            connection.close()
    
    def insert_survey_response(self, session_id: str, question_obj: Dict[str, Any], answer: str) -> bool:
        """
        Insert a single survey response into the database
        
        Args:
            session_id: Unique identifier for the survey session
            question_obj: Question object containing key, label, type, options
            answer: The answer provided by the user/LLM
            
        Returns:
            bool: True if successful, False otherwise
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            # Insert the response
            cursor.execute("""
                INSERT INTO survey_responses 
                (session_id, question_key, question_label, question_type, answer, options)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                session_id,
                question_obj.get('key'),
                question_obj.get('label'),
                question_obj.get('type'),
                answer,
                json.dumps(question_obj.get('options', []))
            ))
            
            # Update or create session record
            cursor.execute("""
                INSERT INTO survey_sessions (session_id, total_questions, completed_questions, status)
                VALUES (%s, 1, 1, 'in_progress')
                ON CONFLICT (session_id) 
                DO UPDATE SET 
                    completed_questions = survey_sessions.completed_questions + 1,
                    updated_at = CURRENT_TIMESTAMP
            """, (session_id,))
            
            connection.commit()
            print(f"Successfully inserted response for question: {question_obj.get('key')}")
            return True
            
        except Exception as e:
            connection.rollback()
            print(f"Error inserting survey response: {e}")
            return False
        finally:
            cursor.close()
            connection.close()
    
    def insert_multiple_responses(self, session_id: str, responses: List[Dict[str, Any]]) -> bool:
        """
        Insert multiple survey responses in a single transaction
        
        Args:
            session_id: Unique identifier for the survey session
            responses: List of dictionaries with 'question' and 'answer' keys
            
        Returns:
            bool: True if successful, False otherwise
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            # Insert all responses
            for response in responses:
                question_obj = response['question']
                answer = response['answer']
                
                cursor.execute("""
                    INSERT INTO survey_responses 
                    (session_id, question_key, question_label, question_type, answer, options)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    session_id,
                    question_obj.get('key'),
                    question_obj.get('label'),
                    question_obj.get('type'),
                    answer,
                    json.dumps(question_obj.get('options', []))
                ))
            
            # Update session record
            cursor.execute("""
                INSERT INTO survey_sessions (session_id, total_questions, completed_questions, status)
                VALUES (%s, %s, %s, 'completed')
                ON CONFLICT (session_id) 
                DO UPDATE SET 
                    completed_questions = %s,
                    status = 'completed',
                    completed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
            """, (session_id, len(responses), len(responses), len(responses)))
            
            connection.commit()
            print(f"Successfully inserted {len(responses)} responses for session: {session_id}")
            return True
            
        except Exception as e:
            connection.rollback()
            print(f"Error inserting multiple responses: {e}")
            return False
        finally:
            cursor.close()
            connection.close()
    
    def get_session_responses(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve all responses for a specific session
        
        Args:
            session_id: Unique identifier for the survey session
            
        Returns:
            List of response dictionaries
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            cursor.execute("""
                SELECT question_key, question_label, question_type, answer, options, created_at
                FROM survey_responses 
                WHERE session_id = %s 
                ORDER BY created_at
            """, (session_id,))
            
            responses = []
            for row in cursor.fetchall():
                responses.append({
                    'question_key': row[0],
                    'question_label': row[1],
                    'question_type': row[2],
                    'answer': row[3],
                    'options': json.loads(row[4]) if row[4] else [],
                    'created_at': row[5].isoformat() if row[5] else None
                })
            
            return responses
            
        except Exception as e:
            print(f"Error retrieving session responses: {e}")
            return []
        finally:
            cursor.close()
            connection.close()
    
    def get_session_summary(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get summary information for a survey session
        
        Args:
            session_id: Unique identifier for the survey session
            
        Returns:
            Dictionary with session summary or None if not found
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            cursor.execute("""
                SELECT total_questions, completed_questions, status, created_at, completed_at
                FROM survey_sessions 
                WHERE session_id = %s
            """, (session_id,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'session_id': session_id,
                    'total_questions': row[0],
                    'completed_questions': row[1],
                    'status': row[2],
                    'created_at': row[3].isoformat() if row[3] else None,
                    'completed_at': row[4].isoformat() if row[4] else None
                }
            return None
            
        except Exception as e:
            print(f"Error retrieving session summary: {e}")
            return None
        finally:
            cursor.close()
            connection.close()


# Example usage:
# from supabase_handler import SupabaseHandler
# from convert_to_json import convert_yaml_to_json_objects
# from llm_question_handler import ask_question_with_llm
# import uuid
# 
# # Initialize database handler
# db_handler = SupabaseHandler()
# db_handler.create_tables_if_not_exist()
# 
# # Convert YAML to questions
# questions = convert_yaml_to_json_objects('sample_q.yml')
# 
# # Generate session ID
# session_id = str(uuid.uuid4())
# 
# # Process questions and store responses
# responses = []
# for question in questions:
#     answer = ask_question_with_llm(question)
#     responses.append({'question': question, 'answer': answer})
# 
# # Store all responses
# success = db_handler.insert_multiple_responses(session_id, responses)
# print(f"Survey completed with session ID: {session_id}") 