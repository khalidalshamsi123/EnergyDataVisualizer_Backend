

app = FastAPI()

# Test comment.

@app.get("/")
async def root():
    return {"message": "Hello World"}






















































































#Line Graph API reader
@app.get("/api/line-graph")
async def get_line_graph():
    #reading the csv file 'Half-Hourly-profiles-ofheating-technologies'
    json_line_graph = pl.read_csv("C:\\Users\\c21086065\\OneDrive - Cardiff University\\Y3-Commercial Frameworks\\dataset\\Spatio-temporal heat demand for LSOAs in England and Wales\\Half-hourly_profiles_of_heating_technologies.csv")
    #creating a dictionary 
    return json_line_graph.to_dicts()
