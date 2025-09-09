import json, os, random, time, logging  # noqa: E401
import numpy as np
import biobricks as bb
from tqdm import tqdm
import chromadb

from dotenv import load_dotenv

from google import genai
from google.genai import types
from google.genai.errors import ClientError, ServerError

load_dotenv()
retries = 6
base_delay = 1.5
jitter = 0.5


def read_metadata(brick: str):
    path = f"cache/{brick}.json"
    if not os.path.exists(path):
        return

    with open(path, "r") as f:
        metadata = json.load(f)

    return metadata


def generate_embeddings(client: genai.Client, metadata, brick: str):
    embeddings, documents = [], []
    pbar = tqdm(metadata, leave=False, position=1)
    model = "gemini-embedding-001"

    if len(pbar) <= 100:
        documents = [json.dumps(asset, separators=(",", ":")) for asset in pbar]

        for attempt in range(retries):
            try:
                resp = client.models.embed_content(
                    model=model,
                    contents=[
                        json.dumps(asset, separators=(",", ":")) for asset in pbar
                    ],
                    config=types.EmbedContentConfig(
                        task_type="RETRIEVAL_DOCUMENT",
                        title=brick,
                    ),
                )
            except ServerError as e:
                if e.code == 503:
                    resp = None
                    print(
                        f"Model overloaded, retrying with {5 - attempt} attempts remaining...",
                    )
                    delay = base_delay * 2**attempt + random.uniform(0, jitter)
                    time.sleep(delay)
                    continue
            except ClientError as e:
                if e.code == 429:
                    resp = None
                    print(
                        f"Quota exceeded, retrying with {5 - attempt} attempts remaining...",
                    )
                    delay = base_delay * 2**attempt + 30
                    time.sleep(delay)
                    continue

        if not resp:
            raise ServerError(503, "Ran out of retries")

        return resp.embeddings, documents

    for asset in pbar:
        pbar.set_description(f"Processing {asset.get('asset')}")

        asset_metadata = json.dumps(asset, separators=(",", ":"))

        for attempt in range(retries):
            try:
                resp = client.models.embed_content(
                    model="gemini-embedding-001",
                    contents=asset_metadata,
                    config=types.EmbedContentConfig(
                        task_type="RETRIEVAL_DOCUMENT",
                        title=f"bb.assets('{asset.get('brick_name')}').{asset.get('asset')}",
                    ),
                )
                break
            except ServerError as e:
                resp = None
                if e.code == 503:
                    print(
                        f"Model overloaded, retrying with {5 - attempt} attempts remaining...",
                    )
                    delay = base_delay * 2**attempt + random.uniform(0, jitter)
                    time.sleep(delay)
                    continue
            except ClientError as e:
                resp = None
                if e.code == 429:
                    print(
                        f"Quota exceeded, retrying with {5 - attempt} attempts remaining...",
                    )
                    delay = base_delay * 2**attempt + 30
                    time.sleep(delay)
                    continue

        if not resp:
            raise ServerError(503, "Ran out of retries")

        documents.append(asset_metadata)
        embeddings.append(resp.embeddings[0].values)

    return embeddings, documents


def read_list():
    client = genai.Client()

    os.makedirs("metadata", exist_ok=True)

    chroma_client = chromadb.PersistentClient(path="metadata")
    collection = chroma_client.get_or_create_collection("bricks")

    with open("list/bricks.txt", "r") as f:
        bricks = [line.strip() for line in f.readlines()]

    pbar = tqdm(bricks, position=0)

    for brick in pbar:
        pbar.set_description(f"Processing {brick}")

        metadata = read_metadata(brick)

        if metadata:
            embeddings, documents = generate_embeddings(client, metadata, brick)

            assert len(embeddings) == len(documents)

            if len(metadata) <= 100:
                collection.add(
                    ids=[str(id) for id in range(len(documents))],
                    embeddings=[embedding.values for embedding in embeddings],
                    documents=documents,
                )
            else:
                collection.add(
                    ids=[str(id) for id in range(len(documents))],
                    embeddings=embeddings,
                    documents=documents,
                )


if __name__ == "__main__":
    read_list()
