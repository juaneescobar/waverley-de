import os
import pandas as pd

from fastapi import FastAPI, Request, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from src.procces_data import (
    validate_tables_schema,
    process_data_employees,
    process_data_departments,
    process_data_job,
    execute_ddl_files,
    connect_mysql
)

app = FastAPI()

templates = Jinja2Templates(directory="templates")

# Custom exception handler for 400 Bad Request
@app.exception_handler(400)
async def bad_request_error(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=400,
        content={"error": "Bad Request", "message": str(exc.detail)}
    )

# Custom exception handler for 404 Not Found
@app.exception_handler(404)
async def not_found_error(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=404,
        content={"error": "Not Found", "message": str(exc.detail)}
    )

# Custom exception handler for 403 Forbidden
@app.exception_handler(403)
async def forbidden_error(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=403,
        content={"error": "Forbidden", "message": str(exc.detail)}
    )

# Custom exception handler for 405 Method Not Allowed
@app.exception_handler(405)
async def method_not_allowed_error(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=405,
        content={"error": "Method Not Allowed", "message": str(exc.detail)}
    )

# Custom exception handler for 500 Internal Server Error
@app.exception_handler(500)
async def internal_error(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "message": str(exc)}
    )

# Example route that could trigger a 404 error
@app.get("/item/{item_id}")
async def get_item(item_id: int):
    if item_id != 1:  # Simulate item not found
        raise HTTPException(status_code=404, detail="Item not found")
    return {"item_id": item_id}

# Example route that could trigger a 400 error
@app.get("/cause-bad-request")
async def cause_bad_request():
    raise HTTPException(status_code=400, detail="This is a bad request")

# Example route that could trigger a 403 error
@app.get("/cause-forbidden")
async def cause_forbidden():
    raise HTTPException(status_code=403, detail="Forbidden access")

# Example route that could trigger a 405 error (method not allowed)
@app.post("/cause-method-not-allowed")
async def cause_method_not_allowed():
    return {"message": "This should not be called with GET"}

@app.post('/upload', methods=['POST'])
async def upload_file(file: UploadFile = File(...)):
    try:
        if not file:
            return JSONResponse(content={'error': 'file not exists'}, status_code=400)
        if file.filename == '':
            return JSONResponse(content={'error': 'No selected file'}, status_code=400)

        conn = connect_mysql()
        cursor = conn.cursor()
        execute_ddl_files(cursor)  # create tables if they don't exist

        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Invalid file extension. Only .csv files are allowed.")

        filename = os.path.basename(file.filename).split(".")[0]
        df = pd.read_csv(file, header=None)
        

        if filename == 'departments':
            # Validate columns lenght
            target_schema = ['id', 'department']
            if validate_tables_schema(df, target_schema):
                return JSONResponse(content={'error': 'Invalid file schema. The columns should be: ' + ', '.join(target_schema)}, status_code=40)

            query = """
                INSERT INTO departments (id, department)
                VALUES (%s, %s)
            """
            data = process_data_departments(df)
            cursor.executemany(query, data)
            conn.commit()
            return JSONResponse(content={'message': 'File uploaded successfully'}, status_code=201)

        elif filename == 'jobs':
            # Validate columns lenght
            target_schema = ['id', 'job']
            if validate_tables_schema(df, target_schema):
                return JSONResponse(content={'error': 'Invalid file schema. The columns should be: ' + ', '.join(target_schema)}, status_code=40)

            query = """
                INSERT INTO jobs (id, job)
                VALUES (%s, %s)
            """
            data = process_data_job(df)
            cursor.executemany(query, data)
            conn.commit()
            return JSONResponse(content={'message': 'File uploaded successfully'}, status_code=201)
        
        elif filename == 'hired_employees':
            target_schema = ['id', 'name', 'datetime', 'department_id', 'job_id']
            if validate_tables_schema(df, target_schema):
                return JSONResponse(content={'error': 'Invalid finumber of columns. The columns should be: ' + ', '.join(target_schema)}, status_code=40)

            query = """
                INSERT INTO hired_employees (id, name, datetime, department_id, job_id)
                VALUES (%s, %s, %s, %s, %s)
            """
            data = process_data_employees(df)
            cursor.executemany(query, data)
            conn.commit()
            return JSONResponse(content={'message': 'File uploaded successfully'}, status_code=201)
        
        else:
            conn.commit()
            return JSONResponse(content={'error': 'Invalid file name. Only hired_employees.csv, departments.csv and jobs.csv are allowed.'}, status_code=400)

    except Exception as e:
        return JSONResponse(content={'error': str(e), 'status': 'Couldnt load the file'}, status_code=500)   

# Root route
@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
