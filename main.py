from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes import maps

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(maps.router)

# Test comment.

@app.get("/")
async def root():
    return {"message": "Hello World"}
