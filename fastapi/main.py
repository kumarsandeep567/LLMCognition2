import os
import time
import base64
import logging
import datetime
import mysql.connector
from openai import OpenAI
from fastapi import FastAPI, status
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import Optional, Any
from mysql.connector import Error
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Custom libraries
from helpers import     \
get_password_hash,      \
verify_password,        \
count_tokens,           \
generate_restriction,   \
rectification_helper,   \
extract_file_content,   \
download_files_from_gcs

# ============================= FastAPI : Begin =============================
# Initialize FastAPI instance
app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins       = ["*"],
    allow_credentials   = True,
    allow_methods       = ["*"],
    allow_headers       = ["*"],
)
# ============================= FastAPI : End ===============================


# Load env variables
load_dotenv()

# Setup OpenAI API key
openai_client = OpenAI(
    api_key         = os.getenv("OPENAI_API"),
    project         = os.getenv("PROJECT_ID"),
    organization    = os.getenv("ORGANIZATION_ID")
)


# ============================= Logger : Begin =============================

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to console (dev only)
if os.getenv('APP_ENV') == "development":
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Also log to a file
file_handler = logging.FileHandler(os.getenv('FASTAPI_LOG_FILE', "fastapi_errors.log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) 

# ============================= Logger : End ===============================


# ====================== Application service : Begin ======================

# Pydantic models for request body validation
class UserRegister(BaseModel):
    first_name: str
    last_name: str
    phone: str
    email: str
    password: str

class UserLogin(BaseModel):
    email: str
    password: str

class PasswordReset(BaseModel):
    first_name: str
    last_name: str
    phone: str
    email: str
    new_password: str

class QueryGPT(BaseModel):
    user_id: int
    task_id: str
    updated_steps: Optional[str] = None

class Feedback(BaseModel):
    user_id: int
    task_id: str
    feedback: str


def create_connection(attempts = 3, delay = 2):
    '''Start a connection with the MySQL database'''

    # Database connection config
    config = {
        'user'              : os.getenv('DB_USER'),
        'password'          : os.getenv('DB_PASSWORD'),
        'host'              : os.getenv('DB_HOST'),
        'database'          : os.getenv('DB_NAME'),
        'raise_on_warnings' : True
    }

    # Attempt a reconnection routine
    attempt = 1
    
    while attempt <= attempts:
        try:
            conn = mysql.connector.connect(**config)
            logger.info("Database - Connection to the database was opened")
            return conn
        
        except (Error, IOError) as error:
            if attempt == attempts:
                # Ran out of attempts
                logger.error(f"Database - Failed to connect to database : {error}")
                return None
            else:
                logger.warning(f"Database - Connection failed: {error} - Retrying {attempt}/{attempts} ...")
                
                # Delay the next attempt
                time.sleep(delay ** attempt)
                attempt += 1
    
    return None


# Route for FastAPI Health check
@app.get("/health")
def health() -> JSONResponse:
    '''Check if the FastAPI application is setup and running'''

    logger.info("GET - /health request received")
    return JSONResponse({
        'status'    : status.HTTP_200_OK,
        'type'      : "string",
        'message'   : "You're viewing a page from FastAPI"
    })


# Route for database health check
@app.get("/database")
def dbhealth() -> JSONResponse:
    '''Check if FastAPI can communicate with the database'''

    logger.info("GET - /database request received")
    conn = create_connection()
    
    if conn is None:
        response = {
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        }
    else:
        response = {
            'status'    : status.HTTP_200_OK,
            'type'      : "string",
            'message'   : "Connection with database established"
        }
        conn.close()
        logger.info("Database - Connection to the database was closed")
    
    return JSONResponse(content=response)


# Route for user registration
@app.post("/register")
def register(user: UserRegister) -> JSONResponse:
    '''Sign up new users to the application'''

    logger.info("POST - /register request received")
    conn = create_connection()

    if conn is None:
        return JSONResponse({
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        })

    if conn and conn.is_connected():
        with conn.cursor(dictionary = True) as cursor:
            try:
                
                # Check if email already exists
                logger.info("SQL - Running a SELECT statement")
                cursor.execute("SELECT * FROM users WHERE email = %s", (user.email,))
                logger.info("SQL - SELECT statement complete")
                
                if cursor.fetchone():
                    conn.close()
                    logger.info("Database - Connection to the database was closed")

                    return JSONResponse({
                        'status'    : status.HTTP_400_BAD_REQUEST,
                        'type'      : "string",
                        'message'   : "Email already registered. Please login."
                    })

                # Hash the password
                hashed_password = get_password_hash(user.password)

                # Insert the new user in the database
                logger.info("SQL - Running an INSERT statement")
                query = """
                INSERT INTO users (first_name, last_name, phone, email, password)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(query, (
                    user.first_name, 
                    user.last_name, 
                    user.phone, 
                    user.email, 
                    hashed_password
                ))
                conn.commit()
                logger.info("SQL - INSERT statement complete")

                new_user_id = cursor.lastrowid
                logger.info(f"New user registered with ID: {new_user_id}")

                response = {
                    "status"    : status.HTTP_200_OK,
                    'type'      : "string",
                    "message"   : "User registered successfully",
                    "user_id"   : new_user_id
                }

            except Exception as exception:
                logger.error("Error: register() encountered an error")
                logger.error(exception)
                response = {
                    "status"    : status.HTTP_500_INTERNAL_SERVER_ERROR,
                    'type'      : "string",
                    "message"   : "New user could not be registered. Something went wrong.",
                }
                
            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")
        
        return JSONResponse(content=response)


# Route for user login
@app.post("/login")
def login(user: UserLogin) -> JSONResponse:
    '''Sign in an existing user'''

    logger.info("POST - /login request received")
    conn = create_connection()

    if conn is None:
        return JSONResponse({
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        })

    if conn and conn.is_connected():
        with conn.cursor(dictionary = True) as cursor:
            try:

                # Fetch user by email
                logger.info("SQL - Running a SELECT statement")
                cursor.execute("SELECT * FROM users WHERE email = %s", (user.email,))
                logger.info("SQL - SELECT statement complete")

                db_user = cursor.fetchone()

                if db_user is None:
                    conn.close()
                    logger.info("Database - Connection to the database was closed")

                    return JSONResponse({
                        'status'    : status.HTTP_404_NOT_FOUND,
                        'type'      : "string",
                        'message'   : "User not found"
                    })

                # Verify password
                if not verify_password(user.password, db_user['password']):
                    response = {
                        'status'    : status.HTTP_401_UNAUTHORIZED,
                        'type'      : "string",
                        'message'   : "Invalid email or password"
                    }
                else:
                    logger.info(f"User logged in: {db_user['user_id']}")
                    response =  {
                        "status"    : status.HTTP_200_OK,
                        'type'      : "string",
                        "message"   : "Login successful",
                        "user_id"   : db_user['user_id']
                    }

            except Exception as exception:
                logger.error("Error: login() encountered an error")
                logger.error(exception)
                response = {
                    "status"    : status.HTTP_500_INTERNAL_SERVER_ERROR,
                    'type'      : "string",
                    "message"   : "User could not be logged in. Something went wrong.",
                }

            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")

        return JSONResponse(content=response)


# Route for password reset
@app.post("/resetpassword")
def reset_password(reset_data: PasswordReset) -> JSONResponse:
    '''Allow users to set a new password if correct details are provided'''

    logger.info("POST - /resetpassword request received")
    conn = create_connection()

    if conn is None:
        return JSONResponse({
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        })

    if conn and conn.is_connected():
        with conn.cursor(dictionary = True) as cursor:
            try:

                # Check if user exists and all provided details match
                logger.info("SQL - Running a SELECT statement")
                query = """
                SELECT * FROM users 
                WHERE first_name = %s 
                AND last_name = %s 
                AND phone = %s 
                AND email = %s
                """
                cursor.execute(query, (
                    reset_data.first_name, 
                    reset_data.last_name, 
                    reset_data.phone, 
                    reset_data.email
                ))
                logger.info("SQL - SELECT statement complete")
                user = cursor.fetchone()

                if user is None:
                    conn.close()
                    logger.info("Database - Connection to the database was closed")

                    return JSONResponse({
                        'status'    : status.HTTP_401_UNAUTHORIZED,
                        'type'      : "string",
                        'message'   : "User not found or details do not match"
                    })
                    
                # Hash the new password
                hashed_password = get_password_hash(reset_data.new_password)

                # Update the password
                logger.info("SQL - Running a UPDATE statement")
                update_query = "UPDATE users SET password = %s WHERE user_id = %s"
                cursor.execute(update_query, (hashed_password, user['user_id']))
                conn.commit()
                logger.info("SQL - UPDATE statement complete")

                logger.info(f"Password reset successful for user ID: {user['user_id']}")

                response = {
                    "status"    : status.HTTP_200_OK,
                    'type'      : "string",
                    "message"   : "Password reset successful"
                }

            except Exception as exception:
                logger.error("Error: reset_password() encountered an error")
                logger.error(exception)
                response = {
                    "status"    : status.HTTP_500_INTERNAL_SERVER_ERROR,
                    'type'      : "string",
                    "message"   : "Password could not be reset. Something went wrong.",
                }

            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")

        return JSONResponse(content=response)


# Route for listing prompts
@app.get("/listprompts")
@app.get("/listprompts/{count}")
def list_prompts(count: Optional[int] = None) -> dict[str, Any]:
    '''Fetch "x" number of prompts from the database'''

    if count is None:
        logger.info("GET - /listprompts request received")
        count = 5
    else:
        logger.info(f"GET - /listprompts/{count} request received")

    conn = create_connection()

    if conn is None:
        return JSONResponse({
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        })

    if conn and conn.is_connected():
        with conn.cursor(dictionary = True) as cursor:
            try:
                logger.info("SQL - Running a SELECT statement")

                # Fetch the task_id, question from the table
                query = f"SELECT `task_id`, `question` FROM `gaia_features` LIMIT {count}"
                result = cursor.execute(query)
                rows = cursor.fetchall()

                response = {
                    'status'    : status.HTTP_200_OK,
                    'type'      : "json",
                    'message'   : rows,
                    'length'    : count
                }

                logger.info("SQL - SELECT statement complete")
                conn.close()
                logger.info("Database - Connection to the database was closed")

                return JSONResponse(content=response)

            except Exception as exception:
                logger.error("Error: list_prompts() encountered a SQL error")
                logger.error(exception)

            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")
    
        return JSONResponse({
            'status'    : status.HTTP_500_INTERNAL_SERVER_ERROR,
            'type'      : "string",
            'message'   : "Could not fetch the list of prompts. Something went wrong."
        })


# Route for fetching all details about a prompt
@app.get("/loadprompt/{task_id}")
def loadprompt(task_id: str) -> JSONResponse:
    '''Load all information from the database regarding the given prompt'''

    logger.info(f"GET - /loadprompt/{task_id} request received")
    conn = create_connection()

    if conn is None:
        return JSONResponse({
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        })

    if conn and conn.is_connected():
        with conn.cursor(dictionary = True) as cursor:
            try:

                # Fetch the task_id, question, level, final_answer, file_name 
                logger.info("SQL - Running a SELECT statement")
                query = """
                SELECT task_id, question, level, final_answer, file_name 
                FROM gaia_features
                WHERE task_id = %s
                """
                result = cursor.execute(query, (task_id,))
                record = cursor.fetchone()
                logger.info("SQL - SELECT statement complete")

                if record is None:
                    conn.close()
                    logger.info("Database - Connection to the database was closed")

                    return JSONResponse({
                        'status'    : status.HTTP_404_NOT_FOUND,
                        'type'      : "string",
                        'message'   : f"Could not fetch the details for the given task_id (not found) {task_id}"
                    })

                conn.close()
                logger.info("Database - Connection to the database was closed")

                return JSONResponse({
                    'status'    : status.HTTP_200_OK,
                    'type'      : "json",
                    'message'   : record
                })

            except Exception as exception:
                logger.error("Error: list_prompts() encountered a SQL error")
                logger.error(exception)

            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")
    
        return JSONResponse({
            'status'    : status.HTTP_500_INTERNAL_SERVER_ERROR,
            'type'      : "string",
            'message'   : "Could not fetch details for the prompt. Something went wrong."
        })


# Route for fetching annotation details for a prompt
@app.get("/getannotation/{task_id}")
def getannotation(task_id: str) -> JSONResponse:
    '''Load the annotation from the database regarding the given prompt'''

    logger.info(f"GET - /loadprompt/{task_id} request received")
    conn = create_connection()

    if conn is None:
        return JSONResponse({
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        })

    if conn and conn.is_connected():
        with conn.cursor(dictionary = True) as cursor:
            try:

                # Fetch the final_answer for the task_id
                logger.info("SQL - Running a SELECT statement")
                query = """SELECT final_answer FROM gaia_features WHERE task_id = %s"""

                result = cursor.execute(query, (task_id,))
                final_answer = cursor.fetchone()
                logger.info("SQL - SELECT statement complete")

                if final_answer is None:
                    conn.close()
                    logger.info("Database - Connection to the database was closed")

                    return JSONResponse({
                        'status'    : status.HTTP_404_NOT_FOUND,
                        'type'      : "string",
                        'message'   : f"Could not fetch the details for the given task_id (not found) {task_id}"
                    })

                # Fetch the steps for the task_id
                logger.info("SQL - Running a SELECT statement")
                query = """SELECT Steps FROM gaia_annotations WHERE task_id = %s"""

                result = cursor.execute(query, (task_id,))
                prompt_steps = cursor.fetchone()
                logger.info("SQL - SELECT statement complete")

                if prompt_steps is None:
                    conn.close()
                    logger.info("Database - Connection to the database was closed")

                    return JSONResponse({
                        'status'    : status.HTTP_404_NOT_FOUND,
                        'type'      : "string",
                        'message'   : f"Could not fetch the annotation steps for the given task_id (not found) {task_id}"
                    })
                
                filtered_prompt = prompt_steps['Steps'].replace(final_answer['final_answer'], '___')

                conn.close()
                logger.info("Database - Connection to the database was closed")

                return JSONResponse({
                    'status'    : status.HTTP_200_OK,
                    'type'      : "string",
                    'message'   : filtered_prompt
                })

            except Exception as exception:
                logger.error("Error: list_prompts() encountered a SQL error")
                logger.error(exception)

            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")
    
        return JSONResponse({
            'status'    : status.HTTP_500_INTERNAL_SERVER_ERROR,
            'type'      : "string",
            'message'   : "Could not fetch details for the prompt. Something went wrong."
        })
    

def update_analytics(data: dict) -> bool:
    '''Save GPT-4's response and some other data to the database'''

    logger.info("INTERNAL - request to save response data to database received")
    conn = create_connection()
    response = False

    if conn and conn.is_connected():
        with conn.cursor(dictionary = True) as cursor:
            try:

                # Update the analytics 
                logger.info("SQL - Running an INSERT statement")

                # Get the columns and the corresponding placeholders
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))

                query = f"INSERT INTO analytics ({columns}) VALUES ({placeholders})"

                cursor.execute(query, tuple(data.values()))
                conn.commit()
                logger.info("SQL - INSERT statement complete")
                response = True

            except Exception as exception:
                logger.error("Error: update_analytics() encountered an error")
                logger.error(exception)
                
            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")
    
    return response


# Route for querying GPT
@app.post("/querygpt")
async def query_gpt(query: QueryGPT) -> JSONResponse:
    '''Forward the question to OpenAI GPT4 and evaluate based on GAIA Benchmark'''

    logger.info(f"POST - /querygpt/{query.task_id} request received")
    
    try:

        # Get the prompt, apply restriction wherever needed, and send to GPT
        prompt = loadprompt(query.task_id)

        if prompt and prompt['status'] == status.HTTP_200_OK:
            
            # If query.updated_steps is empty, then it's a fresh prompt
            if (query.updated_steps is None) or (query.updated_steps == ''):
                
                restriction = generate_restriction(prompt['message']['final_answer'])
                full_question = f"{prompt['message']['question']} {restriction}".strip()
            else:

                # Let GPT know the previous response was incorrect
                rectification = rectification_helper()
                restriction = generate_restriction(prompt['message']['final_answer'])
                full_question = f"{rectification} Question: {prompt['message']['question']} Steps: {query.updated_steps} {restriction}".strip()

            # Prepare the message to send to GPT-4o
            messages = [
                {"role": "system", "content": "You are a helpful assistant that obeys the instructions given and provides the correct answers for any questions provided."},
                {"role": "user", "content": full_question}
            ]

            # Prepare file parsing if available
            file_name = prompt['message']['file_name']
            file_content = None
            content_available = False

            # Download the files if they are not already available
            if not os.path.exists(os.getenv('DOWNLOAD_DIR')):
                content_available = download_files_from_gcs()
            else:
                content_available = True

            if content_available:
                if file_name is not None: 

                    file_path = os.path.join(os.getcwd(), os.getenv('DOWNLOAD_DIR'), file_name)

                    if file_name.lower().endswith(('.png', '.jpg')):

                        # Encode the image to Base64
                        with open(file_path, "rb") as image_file:
                            file_content = base64.b64encode(image_file.read()).decode('utf-8')
                        
                        messages.append({
                            "role": "user",
                            "content": [
                                {"type": "text", "text": "Here's the image related to the question:"},
                                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{file_content}"}}
                            ]
                        })

                    elif file_name.lower().endswith(('.mp3')):

                        audio_file= open(file_path, "rb")
                        try:
                            
                            logger.info("WHISPER - Sending a audio transcription request")
                            file_content = openai_client.audio.transcriptions.create(
                                model = "whisper-1", 
                                file = audio_file,
                                response_format = "text"
                            )

                            if file_content is not None:
                                messages.append({
                                    "role": "user",
                                    "content": f"Here's the transcription of the audio file related to the question: \n {file_content}"
                                })
                        
                        except Exception as exception:
                            logger.error("Error: WHISPER - querygpt() encountered an error")


                    elif file_name.lower().endswith(('.pdf', '.txt', '.xlsx', '.csv', '.jsonld', '.docx', '.py')):
                        
                        # Parse the files
                        file_content = extract_file_content(file_path)
                        
                        if file_content is not None:
                            messages.append({
                                "role": "user",
                                "content": f"Here's the content of the file related to the question: \n\n {file_content}"
                            })

            # Calculate the tokens and cost
            token_count = 0


            for msg in messages:
                if isinstance(msg['content'], str):
                    token_count += count_tokens(msg['content'])
                else:
                    token_count += count_tokens(msg['content'][0]['text'])
                    token_count += count_tokens(file_content) if file_content is not None else 0

            file_token_count = count_tokens(file_content) if file_content is not None else 0
            cost = token_count * 0.000005
            cost = float('{:.4f}'.format(cost))

            # Record the time
            start_time = time.time()

            # Send question to GPT
            logger.info("GPT - Sending a ChatCompletion request")
            response = openai_client.chat.completions.create(
                model = "gpt-4o",
                temperature = 1,
                messages = messages
            )

            logger.info("GPT - ChatCompletion request complete")

            time_consumed = time.time() - start_time
            time_consumed = float('{:.3f}'.format(time_consumed))
            gpt_response = response.choices[0].message.content

            # Save to analytics table
            response_data = {
                "user_id"                   : query.user_id,
                "task_id"                   : prompt['message']['task_id'],
                "gpt_response"              : gpt_response,
                "tokens_per_text_prompt"    : token_count,
                "tokens_per_attachment"     : file_token_count,
                'total_cost'                : cost,
                'time_consumed'             : time_consumed
            }

            if (query.updated_steps is not None) or (query.updated_steps != ''):
                response_data["updated_steps"] = query.updated_steps

            if update_analytics(response_data):
                logger.info("INTERNAL - analytics data saved to database")
            else:
                logger.error("INTERNAL - Failed to save analytics data to database")

            json_response = {
                "status"        : status.HTTP_200_OK,
                "task_id"       : prompt['message']['task_id'],
                "question"      : full_question,
                "level"         : prompt['message']['level'],
                "final_answer"  : prompt['message']['final_answer'],
                "file_name"     : prompt['message']['file_name'],
                "file_content"  : file_content,
                "token_count"   : token_count,
                "file_tokens"   : file_token_count,
                "total_cost"    : cost,
                "gpt_response"  : gpt_response
            }

            # Get the annotation and append it to the json response
            logger.info(f"INTERNAL - Fetching annotation for task_id {prompt['message']['task_id']}")
            annotation = getannotation(prompt['message']['task_id'])

            if annotation["status"] == status.HTTP_200_OK:
                json_response["annotation_steps"] = annotation["message"]

            return JSONResponse(content=json_response)

    except Exception as exception:
        logger.error("Error: querygpt() encountered an error")
        logger.error(exception)

    return JSONResponse({
        'status'    : status.HTTP_500_INTERNAL_SERVER_ERROR,
        'type'      : "string",
        'message'   : "Could not send prompt to GPT. Something went wrong."
    })


# Route for saving feedback GPT
@app.post("/feedback")
def feedback(data: Feedback) -> dict[str, Any]:
    '''Save the user's feedback for GPT's response for the task_id'''

    logger.info(f"POST - /feedback/{data.task_id} request received")

    conn = create_connection()

    if conn is None:
        return JSONResponse({
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        })

    if conn and conn.is_connected():
        with conn.cursor(dictionary = True) as cursor:
            try:

                # Update the analytics and save the feedback
                logger.info("SQL - Running an UPDATE statement")

                query = """
                UPDATE analytics AS a
                JOIN (SELECT id FROM analytics ORDER BY time_stamp DESC LIMIT 1) AS sub
                ON a.id = sub.id
                SET a.feedback = %s
                WHERE a.user_id = %s AND a.task_id = %s
                """

                cursor.execute(query, (data.feedback, data.user_id, data.task_id))
                conn.commit()
                logger.info("SQL - UPDATE statement complete")
                response = {
                    'status'    : status.HTTP_200_OK,
                    'type'      : "string",
                    'message'   : "Feedback saved successfully"
                }

            except Exception as exception:
                logger.error("Error: feedback() encountered an error")
                logger.error(exception)
                response = {
                    'status'    : status.HTTP_500_INTERNAL_SERVER_ERROR,
                    'type'      : "string",
                    'message'   : "Could not save feedback. Something went wrong."
                }
                
            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")
    
        return JSONResponse(content=response)


# Route for analytics
@app.get("/analytics")
async def get_analytics() -> JSONResponse:
    logger.info("GET - /analytics request received")
    conn = create_connection()

    if conn is None:
        return JSONResponse({
            'status'    : status.HTTP_503_SERVICE_UNAVAILABLE,
            'type'      : "string",
            'message'   : "Database not found :("
        })

    if conn and conn.is_connected():
        with conn.cursor(dictionary=True) as cursor:
            try:
                query = """
                SELECT gfeat.*, atx.user_id, atx.updated_steps, atx.tokens_per_text_prompt, 
                       atx.tokens_per_attachment, atx.gpt_response, atx.total_cost, 
                       atx.time_consumed, atx.feedback, atx.time_stamp, afeat.time_taken
                FROM analytics atx, gaia_features gfeat, gaia_annotations afeat
                WHERE atx.task_id = gfeat.task_id AND atx.task_id = afeat.task_id
                """
                cursor.execute(query)
                results = cursor.fetchall()

                # Process the results to ensure they are JSON serializable
                processed_results = []
                for row in results:
                    processed_row = {}
                    for key, value in row.items():
                        if isinstance(value, (int, float, str, type(None))):
                            processed_row[key] = value
                        elif isinstance(value, (datetime.date, datetime.datetime)):
                            processed_row[key] = value.isoformat()
                        else:
                            processed_row[key] = str(value)
                    processed_results.append(processed_row)

                response = {
                    'status'    : status.HTTP_200_OK,
                    'type'      : "json",
                    'message'   : processed_results
                }

            except Exception as exception:
                logger.error("Error: get_analytics() encountered an error")
                logger.error(exception)
                response = {
                    'status'    : status.HTTP_500_INTERNAL_SERVER_ERROR,
                    'type'      : "string",
                    'message'   : "Could not save feedback. Something went wrong."
                }
            
            finally:
                conn.close()
                logger.info("Database - Connection to the database was closed")
        
        return JSONResponse(content=response)

# ====================== Application service : End ======================
