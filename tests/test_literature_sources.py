"""
Unit tests for literature and reagent source Lambda handlers:
  - lambdas/pubmed-search/handler.py        (TestPubMedSearch)
  - lambdas/biorxiv-search/handler.py       (TestBiorxivSearch)
  - lambdas/semantic-scholar-search/handler.py (TestSemanticScholarSearch)
  - lambdas/arxiv-search/handler.py         (TestArxivSearch)
  - lambdas/reagent-search/handler.py       (TestReagentSearch)
  - lambdas/federated-search/handler.py     (TestFederatedSearchLiteratureSources)

All external HTTP calls are mocked via unittest.mock.patch on urllib.request.urlopen.
"""

import importlib
import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_urlopen_response(body):
    """Create a mock urllib response context manager for a JSON body."""
    encoded = json.dumps(body).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = encoded
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def _make_raw_response(raw_bytes: bytes):
    """Create a mock urllib response context manager for raw bytes (e.g. XML)."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = raw_bytes
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def _load_handler(lambda_dir: str, alias: str, env: dict | None = None):
    path = os.path.join(REPO_ROOT, "lambdas", lambda_dir, "handler.py")
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, env or {}):
        spec.loader.exec_module(mod)
    return mod


_pubmed = _load_handler("pubmed-search", "_pubmed_handler")
_biorxiv = _load_handler("biorxiv-search", "_biorxiv_handler")
_s2 = _load_handler("semantic-scholar-search", "_s2_handler")
_arxiv = _load_handler("arxiv-search", "_arxiv_handler")
_reagent = _load_handler("reagent-search", "_reagent_handler")


def _mock_ctx(tool_name: str = "target___pubmed_search"):
    ctx = MagicMock()
    ctx.client_context.custom = {"bedrockAgentCoreToolName": tool_name}
    return ctx


# ---------------------------------------------------------------------------
# PubMed
# ---------------------------------------------------------------------------

_ESEARCH_RESPONSE = {
    "esearchresult": {
        "idlist": ["38000001", "38000002"],
    }
}

_ESUMMARY_RESPONSE = {
    "result": {
        "uids": ["38000001", "38000002"],
        "38000001": {
            "uid": "38000001",
            "title": "Machine Learning for Protein Folding",
            "authors": [{"name": "Alice Smith"}, {"name": "Bob Jones"}],
            "source": "Nature Methods",
            "pubdate": "2026 Jan",
            "epubdate": "2025 Dec 15",
            "pubtype": [{"value": "Journal Article"}],
        },
        "38000002": {
            "uid": "38000002",
            "title": "Deep Neural Networks in Genomics",
            "authors": [{"name": "Carol Lee"}],
            "source": "Genome Research",
            "pubdate": "2025 Mar",
            "epubdate": "",
            "pubtype": [{"value": "Review"}],
        },
    }
}


class TestPubMedSearch:
    def test_happy_path_returns_results(self):
        responses = [
            _make_urlopen_response(_ESEARCH_RESPONSE),
            _make_urlopen_response(_ESUMMARY_RESPONSE),
        ]
        with patch("urllib.request.urlopen", side_effect=responses):
            result = _pubmed.handler({"query": "machine learning protein"}, _mock_ctx())
        assert result["source_type"] == "pubmed"
        assert result["count"] == 2
        r = result["results"][0]
        assert r["pmid"] == "38000001"
        assert r["title"] == "Machine Learning for Protein Folding"
        assert "Alice Smith" in r["authors"]
        assert r["journal"] == "Nature Methods"
        assert r["abstract_text"] == ""
        assert r["mesh_terms"] == []
        assert r["source_type"] == "pubmed"
        assert r["source_id"] == "pubmed/38000001"

    def test_date_filters_included_in_esearch_url(self):
        captured = {}

        def capture_urlopen(req, timeout=None):
            if not captured.get("esearch_url"):
                captured["esearch_url"] = req.get_full_url()
                return _make_urlopen_response(_ESEARCH_RESPONSE)
            return _make_urlopen_response(_ESUMMARY_RESPONSE)

        with patch("urllib.request.urlopen", side_effect=capture_urlopen):
            _pubmed.handler(
                {"query": "cancer", "date_start": "2024/01/01", "date_end": "2024/12/31"},
                _mock_ctx(),
            )
        assert "mindate=2024%2F01%2F01" in captured["esearch_url"] or "mindate=2024/01/01" in captured["esearch_url"]
        assert "datetype=pdat" in captured["esearch_url"]

    def test_max_results_capped_at_50(self):
        big_esearch = {"esearchresult": {"idlist": [str(i) for i in range(60)]}}
        # Only request up to 50
        with patch("urllib.request.urlopen", side_effect=[
            _make_urlopen_response(big_esearch),
            _make_urlopen_response({"result": {"uids": []}}),
        ]):
            result = _pubmed.handler({"query": "cancer", "max_results": 100}, _mock_ctx())
        assert result["count"] <= 50

    def test_esearch_http_error_returns_empty(self):
        import urllib.error
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            url="https://eutils.ncbi.nlm.nih.gov", code=429, msg="Too Many Requests", hdrs={}, fp=None
        )):
            result = _pubmed.handler({"query": "cancer"}, _mock_ctx())
        assert "error" not in result
        assert result["count"] == 0
        assert result["results"] == []

    def test_empty_query_returns_error(self):
        result = _pubmed.handler({}, _mock_ctx())
        assert "error" in result
        assert "query" in result["error"]

    def test_pub_type_filter_excludes_non_matching(self):
        responses = [
            _make_urlopen_response(_ESEARCH_RESPONSE),
            _make_urlopen_response(_ESUMMARY_RESPONSE),
        ]
        with patch("urllib.request.urlopen", side_effect=responses):
            result = _pubmed.handler(
                {"query": "machine learning", "pub_type_filter": "Journal Article"},
                _mock_ctx(),
            )
        # Only the first record is a Journal Article
        pmids = [r["pmid"] for r in result["results"]]
        assert "38000001" in pmids
        assert "38000002" not in pmids

    def test_recency_score_current_year(self):
        # uid pubdate starts with 2026 → score 1.0
        responses = [
            _make_urlopen_response({"esearchresult": {"idlist": ["38000001"]}}),
            _make_urlopen_response({
                "result": {
                    "uids": ["38000001"],
                    "38000001": {
                        "uid": "38000001", "title": "Test", "authors": [],
                        "source": "J", "pubdate": "2026 Jan",
                        "epubdate": "", "pubtype": [],
                    },
                }
            }),
        ]
        with patch("urllib.request.urlopen", side_effect=responses):
            result = _pubmed.handler({"query": "test"}, _mock_ctx())
        assert result["results"][0]["quality_score"] == 1.0


# ---------------------------------------------------------------------------
# bioRxiv
# ---------------------------------------------------------------------------

_BIORXIV_COLLECTION = {
    "collection": [
        {
            "doi": "10.1101/2026.01.01.000001",
            "title": "Machine Learning for Single-Cell RNA Sequencing",
            "authors": "Alice Smith; Bob Jones",
            "category": "bioinformatics",
            "date": "2026-01-01",
            "abstract": "We present a deep learning approach to cluster single-cell RNA-seq data.",
        },
        {
            "doi": "10.1101/2026.01.02.000002",
            "title": "CRISPR Screen in Mouse Neurons",
            "authors": "Carol Lee",
            "category": "neuroscience",
            "date": "2026-01-02",
            "abstract": "Genome-wide CRISPR screen identifies regulators of neuronal activity.",
        },
        {
            "doi": "10.1101/2026.01.03.000003",
            "title": "Unrelated Physics Paper",
            "authors": "Dave Green",
            "category": "physics",
            "date": "2026-01-03",
            "abstract": "A paper with no biology content whatsoever.",
        },
    ]
}


class TestBiorxivSearch:
    def test_happy_path_returns_keyword_filtered_results(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_BIORXIV_COLLECTION)):
            result = _biorxiv.handler({"query": "machine learning RNA"}, _mock_ctx())
        assert result["source_type"] == "biorxiv"
        assert result["count"] >= 1
        titles = [r["title"] for r in result["results"]]
        assert "Machine Learning for Single-Cell RNA Sequencing" in titles

    def test_server_both_makes_two_calls(self):
        call_urls = []

        def capture(req, timeout=None):
            call_urls.append(req.get_full_url())
            return _make_urlopen_response({"collection": []})

        with patch("urllib.request.urlopen", side_effect=capture):
            _biorxiv.handler({"query": "RNA", "server": "both"}, _mock_ctx())

        assert len(call_urls) == 2
        assert any("biorxiv" in u for u in call_urls)
        assert any("medrxiv" in u for u in call_urls)

    def test_date_filter_reflected_in_url(self):
        captured = {}

        def capture(req, timeout=None):
            captured["url"] = req.get_full_url()
            return _make_urlopen_response({"collection": []})

        with patch("urllib.request.urlopen", side_effect=capture):
            _biorxiv.handler(
                {"query": "RNA", "date_start": "2026-01-01", "date_end": "2026-03-31"},
                _mock_ctx(),
            )
        assert "2026-01-01" in captured["url"]
        assert "2026-03-31" in captured["url"]

    def test_empty_collection_returns_zero_count(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response({"collection": []})):
            result = _biorxiv.handler({"query": "RNA"}, _mock_ctx())
        assert result["count"] == 0

    def test_empty_query_returns_error(self):
        result = _biorxiv.handler({}, _mock_ctx())
        assert "error" in result

    def test_invalid_server_returns_error(self):
        result = _biorxiv.handler({"query": "RNA", "server": "not_a_server"}, _mock_ctx())
        assert "error" in result

    def test_abstract_capped_at_500_chars(self):
        long_abstract = "biology " * 200
        coll = {"collection": [{"doi": "x", "title": "biology study", "authors": "A",
                                 "category": "bio", "date": "2026-01-01",
                                 "abstract": long_abstract}]}
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(coll)):
            result = _biorxiv.handler({"query": "biology"}, _mock_ctx())
        for r in result["results"]:
            assert len(r["abstract"]) <= 500


# ---------------------------------------------------------------------------
# Semantic Scholar
# ---------------------------------------------------------------------------

_S2_RESPONSE = {
    "data": [
        {
            "paperId": "abc123",
            "title": "Attention Is All You Need",
            "authors": [{"name": "Vaswani"}, {"name": "Shazeer"}],
            "year": 2017,
            "citationCount": 80000,
            "fieldsOfStudy": ["Computer Science"],
            "abstract": "We propose a new simple network architecture, the Transformer.",
        },
        {
            "paperId": "def456",
            "title": "BERT Pre-training of Deep Bidirectional Transformers",
            "authors": [{"name": "Devlin"}],
            "year": 2019,
            "citationCount": 60000,
            "fieldsOfStudy": ["Computer Science"],
            "abstract": "We introduce BERT for language representation.",
        },
        {
            "paperId": "ghi789",
            "title": "Low-Citation Machine Learning Paper",
            "authors": [{"name": "Unknown"}],
            "year": 2024,
            "citationCount": 2,
            "fieldsOfStudy": ["Computer Science"],
            "abstract": "A paper with very few citations.",
        },
    ]
}


class TestSemanticScholarSearch:
    def test_happy_path_returns_results(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_S2_RESPONSE)):
            result = _s2.handler({"query": "transformer attention"}, _mock_ctx())
        assert result["source_type"] == "semantic_scholar"
        assert result["count"] >= 1
        r = result["results"][0]
        assert r["paper_id"] == "abc123"
        assert "Vaswani" in r["authors"]
        assert r["source_type"] == "semantic_scholar"
        assert r["source_id"] == "s2/abc123"
        assert "quality_score" in r

    def test_min_citations_filter_applied(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_S2_RESPONSE)):
            result = _s2.handler({"query": "machine learning", "min_citations": 50000}, _mock_ctx())
        # Only 2 papers have >= 50000 citations
        assert result["count"] == 2

    def test_fields_of_study_filter_string(self):
        response = {
            "data": [
                {
                    "paperId": "bio001",
                    "title": "Protein Folding Study",
                    "authors": [],
                    "year": 2023,
                    "citationCount": 10,
                    "fieldsOfStudy": ["Biology"],
                    "abstract": "A biology paper.",
                },
                {
                    "paperId": "cs001",
                    "title": "Sorting Algorithms",
                    "authors": [],
                    "year": 2023,
                    "citationCount": 5,
                    "fieldsOfStudy": ["Computer Science"],
                    "abstract": "A CS paper.",
                },
            ]
        }
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(response)):
            result = _s2.handler({"query": "study", "fields_of_study": "Biology"}, _mock_ctx())
        ids = [r["paper_id"] for r in result["results"]]
        assert "bio001" in ids
        assert "cs001" not in ids

    def test_year_filter_applied(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_S2_RESPONSE)):
            result = _s2.handler({"query": "machine learning", "year_start": 2019}, _mock_ctx())
        for r in result["results"]:
            assert r["year"] is None or r["year"] >= 2019

    def test_empty_query_returns_error(self):
        result = _s2.handler({}, _mock_ctx())
        assert "error" in result

    def test_abstract_capped_at_500_chars(self):
        response = {
            "data": [{
                "paperId": "x1", "title": "test", "authors": [], "year": 2023,
                "citationCount": 0, "fieldsOfStudy": [], "abstract": "word " * 300,
            }]
        }
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(response)):
            result = _s2.handler({"query": "test"}, _mock_ctx())
        for r in result["results"]:
            assert len(r["abstract"]) <= 500

    def test_api_error_returns_empty(self):
        import urllib.error
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            url="https://api.semanticscholar.org", code=429, msg="Too Many Requests", hdrs={}, fp=None
        )):
            result = _s2.handler({"query": "transformer"}, _mock_ctx())
        assert "error" not in result
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# arXiv
# ---------------------------------------------------------------------------

_ARXIV_ATOM_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2301.00001v1</id>
    <title>Test Paper on Machine Learning</title>
    <author><name>Alice Smith</name></author>
    <author><name>Bob Jones</name></author>
    <published>2023-01-01T00:00:00Z</published>
    <summary>A comprehensive study of machine learning algorithms.</summary>
    <category term="cs.LG"/>
    <category term="cs.AI"/>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2301.00002v1</id>
    <title>Quantum Entanglement Experiments</title>
    <author><name>Carol Lee</name></author>
    <published>2023-01-02T00:00:00Z</published>
    <summary>Experimental study of quantum entanglement in photon pairs.</summary>
    <category term="quant-ph"/>
  </entry>
</feed>"""


class TestArxivSearch:
    def test_happy_path_returns_results(self):
        with patch("urllib.request.urlopen", return_value=_make_raw_response(_ARXIV_ATOM_XML)):
            result = _arxiv.handler({"query": "machine learning"}, _mock_ctx())
        assert result["source_type"] == "arxiv"
        assert result["count"] >= 1
        r = result["results"][0]
        assert r["arxiv_id"] == "2301.00001v1"
        assert r["title"] == "Test Paper on Machine Learning"
        assert "Alice Smith" in r["authors"]
        assert "Bob Jones" in r["authors"]
        assert r["published"] == "2023-01-01T00:00:00Z"
        assert r["source_type"] == "arxiv"
        assert r["source_id"] == "arxiv/2301.00001v1"
        assert "cs.LG" in r["categories"]

    def test_category_filter_applied(self):
        with patch("urllib.request.urlopen", return_value=_make_raw_response(_ARXIV_ATOM_XML)):
            result = _arxiv.handler({"query": "study", "category_filter": "cs.LG"}, _mock_ctx())
        ids = [r["arxiv_id"] for r in result["results"]]
        assert "2301.00001v1" in ids
        assert "2301.00002v1" not in ids

    def test_empty_query_returns_error(self):
        result = _arxiv.handler({}, _mock_ctx())
        assert "error" in result

    def test_max_results_capped_at_50(self):
        with patch("urllib.request.urlopen", return_value=_make_raw_response(_ARXIV_ATOM_XML)):
            result = _arxiv.handler({"query": "machine", "max_results": 200}, _mock_ctx())
        assert result["count"] <= 50

    def test_http_error_returns_empty(self):
        import urllib.error
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            url="http://export.arxiv.org", code=503, msg="Service Unavailable", hdrs={}, fp=None
        )):
            result = _arxiv.handler({"query": "machine learning"}, _mock_ctx())
        assert "error" not in result
        assert result["count"] == 0

    def test_summary_capped_at_500_chars(self):
        long_summary = "machine " * 200
        long_xml = (
            b'<?xml version="1.0" encoding="UTF-8"?>'
            b'<feed xmlns="http://www.w3.org/2005/Atom">'
            b"<entry>"
            b"<id>http://arxiv.org/abs/2301.99999v1</id>"
            b"<title>machine learning study</title>"
            b"<author><name>X</name></author>"
            b"<published>2023-01-01T00:00:00Z</published>"
            b"<summary>" + long_summary.encode() + b"</summary>"
            b"<category term='cs.LG'/>"
            b"</entry></feed>"
        )
        with patch("urllib.request.urlopen", return_value=_make_raw_response(long_xml)):
            result = _arxiv.handler({"query": "machine"}, _mock_ctx())
        for r in result["results"]:
            assert len(r["summary"]) <= 500


# ---------------------------------------------------------------------------
# Reagent Search
# ---------------------------------------------------------------------------

_ADDGENE_RESPONSE = {
    "results": [
        {
            "id": 12345,
            "name": "pLenti-CMV-GFP-Puro",
            "organism": "human",
            "description": "Lentiviral vector for GFP expression with puromycin resistance",
            "url": "https://www.addgene.org/12345/",
            "type": "plasmid",
        },
        {
            "id": 67890,
            "name": "Cas9-mCherry",
            "organism": "mouse",
            "description": "CRISPR Cas9 fused to mCherry fluorescent protein",
            "url": "https://www.addgene.org/67890/",
            "type": "plasmid",
        },
    ]
}


class TestReagentSearch:
    def test_no_api_key_returns_note(self):
        # Load with no API key
        result = _reagent.handler({"query": "GFP plasmid"}, _mock_ctx())
        assert result["source_type"] == "reagents"
        assert result["count"] == 0
        assert result["results"] == []
        assert "note" in result
        assert "ADDGENE_API_KEY" in result["note"]

    def test_with_api_key_calls_endpoint(self):
        _reagent_with_key = _load_handler(
            "reagent-search", "_reagent_handler_keyed",
            env={"ADDGENE_API_KEY": "testtoken123"},
        )
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_ADDGENE_RESPONSE)):
            result = _reagent_with_key.handler({"query": "GFP plasmid"}, _mock_ctx())
        assert result["source_type"] == "reagents"
        assert result["count"] == 2
        r = result["results"][0]
        assert r["reagent_id"] == "12345"
        assert r["name"] == "pLenti-CMV-GFP-Puro"
        assert r["source_type"] == "reagents"
        assert r["source_id"] == "reagent/12345"

    def test_empty_query_returns_error(self):
        result = _reagent.handler({}, _mock_ctx())
        assert "error" in result

    def test_invalid_reagent_type_returns_error(self):
        result = _reagent.handler({"query": "GFP", "reagent_type": "virus"}, _mock_ctx())
        assert "error" in result

    def test_max_results_capped_at_50(self):
        big_response = {
            "results": [
                {"id": i, "name": f"Plasmid{i}", "organism": "human",
                 "description": "GFP vector", "url": f"https://www.addgene.org/{i}/", "type": "plasmid"}
                for i in range(60)
            ]
        }
        _reagent_with_key2 = _load_handler(
            "reagent-search", "_reagent_handler_keyed2",
            env={"ADDGENE_API_KEY": "testtoken456"},
        )
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(big_response)):
            result = _reagent_with_key2.handler({"query": "GFP", "max_results": 100}, _mock_ctx())
        assert result["count"] <= 50


# ---------------------------------------------------------------------------
# Federated search dispatch for literature sources
# ---------------------------------------------------------------------------

def _load_federated():
    path = os.path.join(REPO_ROOT, "lambdas", "federated-search", "handler.py")
    alias = "_fed_handler_literature"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, {"REGISTRY_TABLE": "qs-data-source-registry", "CATALOG_TABLE": "qs-catalog"}):
        spec.loader.exec_module(mod)
    return mod


_fed = _load_federated()


def _fed_ctx():
    ctx = MagicMock()
    ctx.client_context.custom = {"bedrockAgentCoreToolName": "t___federated_search"}
    return ctx


def _make_source(source_id: str, source_type: str, display_name: str = "") -> dict:
    return {
        "source_id": source_id,
        "type": source_type,
        "display_name": display_name or source_type,
        "description": f"{source_type} data source",
        "data_classification": "public",
    }


class TestFederatedSearchLiteratureSources:
    def _run_with_source(self, source: dict, fn_name: str, mock_return: list):
        mock_reg = MagicMock()
        mock_reg.scan.return_value = {"Items": [source]}
        with patch.object(_fed, "dynamodb") as mock_ddb, \
             patch.object(_fed, fn_name, return_value=mock_return) as mock_fn:
            mock_ddb.Table.return_value = mock_reg
            result = _fed.handler({"query": "test query", "caller_clearance": "public"}, _fed_ctx())
        return result, mock_fn

    def test_pubmed_source_dispatches(self):
        source = _make_source("pubmed-main", "pubmed", "PubMed")
        mock_hit = {
            "source_id": "pubmed-main", "source_type": "pubmed",
            "display_name": "ML Paper", "match_score": 0.8,
            "description": "machine learning", "quality_score": 1.0,
        }
        result, mock_fn = self._run_with_source(source, "_search_pubmed", [mock_hit])
        mock_fn.assert_called_once()
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "pubmed"

    def test_biorxiv_source_dispatches(self):
        source = _make_source("biorxiv-main", "biorxiv", "bioRxiv")
        mock_hit = {
            "source_id": "biorxiv-main", "source_type": "biorxiv",
            "display_name": "RNA preprint", "match_score": 0.7,
            "description": "single cell rna seq", "quality_score": None,
        }
        result, mock_fn = self._run_with_source(source, "_search_biorxiv", [mock_hit])
        mock_fn.assert_called_once()
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "biorxiv"

    def test_semantic_scholar_source_dispatches(self):
        source = _make_source("s2-main", "semantic_scholar", "Semantic Scholar")
        mock_hit = {
            "source_id": "s2-main", "source_type": "semantic_scholar",
            "display_name": "Attention Paper", "match_score": 1.0,
            "description": "transformer", "quality_score": 0.9,
        }
        result, mock_fn = self._run_with_source(source, "_search_semantic_scholar", [mock_hit])
        mock_fn.assert_called_once()
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "semantic_scholar"

    def test_arxiv_source_dispatches(self):
        source = _make_source("arxiv-main", "arxiv", "arXiv")
        mock_hit = {
            "source_id": "arxiv-main", "source_type": "arxiv",
            "display_name": "arXiv preprint", "match_score": 0.5,
            "description": "cs.LG paper", "quality_score": None,
        }
        result, mock_fn = self._run_with_source(source, "_search_arxiv", [mock_hit])
        mock_fn.assert_called_once()
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "arxiv"

    def test_reagents_source_dispatches(self):
        source = _make_source("addgene-main", "reagents", "Addgene")
        mock_hit = {
            "source_id": "addgene-main", "source_type": "reagents",
            "display_name": "GFP plasmid", "match_score": 0.9,
            "description": "lentiviral vector", "quality_score": None,
        }
        result, mock_fn = self._run_with_source(source, "_search_reagents", [mock_hit])
        mock_fn.assert_called_once()
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "reagents"

    def test_unknown_source_type_skipped(self):
        source = _make_source("unknown-src", "unknown_api")
        mock_reg = MagicMock()
        mock_reg.scan.return_value = {"Items": [source]}
        with patch.object(_fed, "dynamodb") as mock_ddb:
            mock_ddb.Table.return_value = mock_reg
            result = _fed.handler({"query": "anything"}, _fed_ctx())
        assert result["total"] == 0
