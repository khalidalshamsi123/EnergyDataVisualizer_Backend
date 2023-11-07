from fastapi import FastAPI

app = FastAPI()

# Test comment.

@app.get("/")
async def root():
    return {"message": "Hello World"}
