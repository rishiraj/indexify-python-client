# Getting Started

This is a simple example showing how we can load in wikipedia entries into an Indexify server to create a simple RAG application.

Note that you will need to use 3 different terminals to run this example

1. Terminal 1 to execute python scripts
2. Terminal 2 to run the indexify server
3. Terminal 3 to run the indexify extractors

## Steps

1. (Terminal 1) Create a virtual environment and install the requirements

```bash
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

2. (Terminal 1 ) Download the latest version of the indexify server and start the server

```bash
curl https://getindexify.ai | sh
./indexify server -d
```

3. (Terminal 2) Download the Chunking and Embedding extractors

```bash
source venv/bin/activate
indexify-extractor download hub://embedding/minilm-l6
indexify-extractor download hub://text/chunking
```

4. (Terminal 2 ) Start the extractors using the `indexify-extractor` cli tool

```bash
indexify-extractor join-server
```

5. (Terminal 3) Create the extraction graph and load in the wikipedia entries into Indexify. This will download ~20 pages of data from Wikipedia, chunk it and then embed it automatically using the `minilm-16` model.

```bash
source venv/bin/activate
python3 ./setup.py
```

6. (Terminal 3) Once we've loaded in the wikipedia entries, we can now perform a simple RAG query on the index that we've created

```
python3 ./query.py
```
