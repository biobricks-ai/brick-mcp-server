import json, os
import numpy as np
import biobricks as bb
from tqdm import tqdm

from dotenv import load_dotenv

from google import genai
from google.genai import types


def read_metadata(brick):
    with open(f"cache/{brick}.json", "r") as f:
        metadata = json.load(f)

    return metadata


def generate_embeddings(client, metadata):
    embeddings = []
    for idx, entry in enumerate(metadata):
        text = json.dumps(entry, separators=(",", ":"))
        resp = client.models.embed_content(
            model="gemini-embedding-001",
            contents=text,
            config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
        )
        embeddings.append(np.array(resp.embeddings))

    embeddings = np.vstack(embeddings)

    return embeddings


def write_embeddings(brick, metadata, embeddings):
    new_metadata = {
        "metadata": metadata,
        "embeddings": embeddings,
    }

    with open(f"metadata/{brick}.json", "w") as f_out:
        json.dump(new_metadata, f_out, separators=(",", ":"), default=str)


def read_list():
    load_dotenv()
    client = genai.Client()

    os.makedirs("metadata", exist_ok=True)

    with open("list/bricks.txt", "r") as f:
        for line in tqdm(f.readlines(), desc="Processing bricks", position=0):
            brick = line.strip()

            metadata = read_metadata(brick)

            embeddings = generate_embeddings(client, metadata)

            write_embeddings(brick, metadata, embeddings)
