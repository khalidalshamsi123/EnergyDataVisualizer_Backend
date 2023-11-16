from fastapi import FastAPI

from routes import tables

app = FastAPI()

app.include_router(tables.router)

# Test comment.

@app.get("/")
async def root():
    return {"message": "Hello World"}
