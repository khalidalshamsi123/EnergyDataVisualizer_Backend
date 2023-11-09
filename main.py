from fastapi import FastAPI
import polars as pl

app = FastAPI()

# Test comment.

@app.get("/")
async def root():
    return {"message": "Hello World"}









@app.get("/api/line-graph")
async def get_line_graph():
    #reading the csv file
    json_line_graph = pl.read_csv("C:\\Users\\c21086065\\OneDrive - Cardiff University\\Y3-Commercial Frameworks\\dataset\\Spatio-temporal heat demand for LSOAs in England and Wales\\Half-hourly_profiles_of_heating_technologies.csv")
    #creating a dictionary 
    return json_line_graph.to_dicts()
