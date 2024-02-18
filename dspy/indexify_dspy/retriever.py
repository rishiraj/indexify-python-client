from typing import Optional, Union
from indexify import IndexifyClient
import dspy


class IndexifyRM(dspy.Retrieve):
    """A retrieval module that uses Indexify to return the top passages for a given query.
    Assumes indexify client has been created and populated.
    """

    def __init__(
        self,
        indexify_client: IndexifyClient,
        k: int = 3,
    ):
        """Initialize the IndexifyRM."""
        self._indexify_client = indexify_client
        super().__init__(k=k)

    def forward(self, query_or_queries: Union[str, list[str]], index_name: str, k: Optional[int]) -> dspy.Prediction:
        """Indexify index and search."""
        queries = [query_or_queries] if isinstance(query_or_queries, str) else query_or_queries
        queries = [q for q in queries if q]  # Filter empty queries
        k = k if k is not None else self.k

        results = []
        for query in queries:
            response = self._indexify_client.search_index(index_name, query, k)
            results.extend(response)

        return dspy.Prediction(
            passages=[result["text"] for result in results],
        )


if __name__ == "__main__":
    indexify_client = IndexifyClient()
    indexify_client.add_documents(
        [
            "Indexify is amazing!",
            "Indexify is a retrieval service for LLM agents!",
            "Steph Curry is the best basketball player in the world.",
        ],
    )

    indexify_client.bind_extractor(extractor="tensorlake/minilm-l6", 
                                   name="minilml6", 
                                   content_source="ingestion")

    bindings = indexify_client.extractor_bindings
    
    print(bindings)
    retrieve = IndexifyRM(indexify_client)
    topk_passages = retrieve("Sports", "minil6", 2).passages