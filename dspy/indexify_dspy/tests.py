from indexify import IndexifyClient
from retriever import IndexifyRM

if __name__ == "__main__":
    indexify_client = IndexifyClient()
    indexify_client.add_documents(
        [
            "Indexify is amazing!",
            "Indexify is a retrieval service for LLM agents!",
            "Steph Curry is the best basketball player in the world.",
        ],
    )

    indexify_client.add_extraction_policy(
        extractor="tensorlake/minilm-l6", name="minilml6", content_source="ingestion"
    )

    retrieve = IndexifyRM(indexify_client)
    k = 2
    topk_passages = retrieve("Sports", "minilml6.embedding", k).passages
    print(topk_passages)
    assert len(topk_passages) == k
