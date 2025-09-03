from fastapi import FastAPI
from mcp_helpers import list_bricks, get_preview, search_bricks

app = FastAPI()


@app.get("/list_bricks")
def api_list_bricks():
    return list_bricks()


@app.get("/get_preview/{brick_name}")
def api_get_preview(repo_name: str, asset_name: str = None):
    return get_preview(repo_name, asset_name)


@app.get("/search_bricks")
def api_search_bricks(query: str):
    return search_bricks(query)
