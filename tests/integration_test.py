from indexify.client import IndexifyClient, Document, ExtractorBinding
import time
import os
import unittest


class TestIntegrationTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIntegrationTest, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.client = IndexifyClient()

    def test_list_namespaces(self):
        client = IndexifyClient()
        namespaces = client.namespaces()
        assert len(namespaces) >= 1

    def test_get_namespace(self):
        namespace = "default"
        client = IndexifyClient(namespace=namespace)
        assert client.namespace == namespace

    def test_create_namespace(self):
        namespace_name = "test.createnamespace"

        minilm_binding = ExtractorBinding(
            extractor="tensorlake/minilm-l6",
            name="minilm-l6",
            content_source="source",
            input_params={},
        )

        client = IndexifyClient.create_namespace(
            namespace_name, extractor_bindings=[minilm_binding]
        )
        assert client.namespace == namespace_name

    def test_add_documents(self):
        # Add single documents
        namespace_name = "test.adddocuments"
        client = IndexifyClient.create_namespace(namespace_name)

        client.add_documents(
            Document(
                text="This is a test",
                labels={"source": "test"},
            )
        )

        # Add multiple documents
        client.add_documents(
            [
                Document(
                    text="This is a new test",
                    labels={"source": "test"},
                ),
                Document(
                    text="This is another test",
                    labels={"source": "test"},
                ),
            ]
        )

        # Add single string
        client.add_documents("test")

        # Add multiple strings
        client.add_documents(["one", "two", "three"])

        # Add mixed
        client.add_documents(["string", Document("document string", {})])

    def test_get_content(self):
        namespace_name = "test.getcontent"
        client = IndexifyClient.create_namespace(namespace=namespace_name)
        client.add_documents(
            [Document(text="one", labels={"l1": "test"}), "two", "three"]
        )
        content = client.get_content()
        assert len(content) == 3

        # parent doesn't exist
        content = client.get_content(parent_id="idontexist")
        assert len(content) == 0

        # filter label
        content = client.get_content(labels_eq="l1:test")
        assert len(content) == 1

    def test_search(self):
        namespace_name = "test.search2"
        extractor_name = "minilml6_test_search"

        client = IndexifyClient.create_namespace(namespace_name)
        source = "test"

        client.bind_extractor(
            extractor="tensorlake/minilm-l6",
            name=extractor_name,
            labels_eq=f"source:test",
        )

        client.add_documents(
            [
                Document(
                    text="Indexify is also a retrieval service for LLM agents!",
                    labels={"source": source},
                )
            ]
        )
        time.sleep(10)
        results = client.search_index(f"{extractor_name}.embedding", "LLM", 1)
        assert len(results) == 1

    def test_list_extractors(self):
        extractors = self.client.extractors()
        assert len(extractors) >= 1

    def test_bind_extractor(self):
        name = "minilml6_test_bind_extractor"
        namespace_name = "test.bindextractor"
        client = IndexifyClient.create_namespace(namespace_name)
        client.bind_extractor(
            "tensorlake/minilm-l6",
            name,
        )

    def test_query_metadata(self):
        namespace_name = "test.querymetadata"
        name = "minilml6_test_query_metadata"
        client = IndexifyClient.create_namespace(namespace_name)
        client.add_documents("test")
        content_id = client.get_content()[0]["id"]
        client.bind_extractor(
            "tensorlake/minilm-l6",
            name,
        )

        for index in client.indexes():
            index_name = index.get("name")
            client.query_metadata(index_name, content_id)
            # TODO: validate response - currently broken

    def test_extractor_input_params(self):
        name = "minilml6_test_extractor_input_params"
        client = IndexifyClient.create_namespace(namespace="test.extractorinputparams")
        client.bind_extractor(
            extractor="tensorlake/minilm-l6",
            name=name,
            input_params={
                "chunk_size": 300,
                "overlap": 50,
                "text_splitter": "char",
            },
        )

    def test_get_bindings(self):
        name = "minilml6_test_get_bindings"
        client = IndexifyClient.create_namespace("test.getbindings")
        client.bind_extractor(
            "tensorlake/minilm-l6",
            name,
        )
        bindings = client.extractor_bindings
        assert len(list(filter(lambda x: x.name.startswith(name), bindings))) == 1

    def test_get_indexes(self):
        name = "minilml6_test_get_indexes"
        client = IndexifyClient.create_namespace("test.getindexes")
        client.bind_extractor(
            "tensorlake/minilm-l6",
            name,
        )
        indexes = client.indexes()
        assert len(list(filter(lambda x: x.get("name").startswith(name), indexes))) == 1

    def test_upload_file(self):
        test_file_path = os.path.join(os.path.dirname(__file__), "files", "test.txt")
        self.client.upload_file(test_file_path)


if __name__ == "__main__":
    unittest.main()
