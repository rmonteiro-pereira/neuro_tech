"""Tests for fuzzy column-name matching (schema alignment across years)."""
from iptu_pipeline.utils.column_matcher import (
    KNOWN_COLUMN_MAPPINGS,
    find_similar_columns,
    match_and_map_columns,
    similarity_score,
)


class TestSimilarityScore:
    def test_identical_strings_score_one(self):
        assert similarity_score("valor IPTU", "valor IPTU") == 1.0

    def test_case_insensitive(self):
        assert similarity_score("VALOR IPTU", "valor iptu") == 1.0

    def test_unrelated_strings_score_low(self):
        assert similarity_score("bairro", "xyz123") < 0.3

    def test_similar_strings_score_between(self):
        score = similarity_score("quantidade de pavimentos", "quant pavimentos")
        assert 0.7 < score < 1.0


class TestFindSimilarColumns:
    def test_known_mapping_gets_perfect_score(self):
        matches = find_similar_columns(
            source_columns=["quantidade de pavimentos"],
            target_columns=["quant pavimentos"],
            known_mappings=KNOWN_COLUMN_MAPPINGS,
        )
        assert matches["quantidade de pavimentos"] == ("quant pavimentos", 1.0)

    def test_threshold_excludes_weak_matches(self):
        matches = find_similar_columns(
            source_columns=["abcdef"],
            target_columns=["uvwxyz"],
            threshold=0.7,
        )
        assert matches == {}

    def test_exact_match_is_skipped(self):
        # Columns already present in the target need no mapping
        matches = find_similar_columns(
            source_columns=["bairro"],
            target_columns=["bairro"],
        )
        assert "bairro" not in matches


class TestMatchAndMapColumns:
    def test_known_mapping_pavimentos(self):
        mapping = match_and_map_columns(
            source_columns=["quantidade de pavimentos", "bairro"],
            target_columns=["quant pavimentos", "bairro"],
        )
        assert mapping == {"quantidade de pavimentos": "quant pavimentos"}

    def test_known_mapping_valor_iptu(self):
        mapping = match_and_map_columns(
            source_columns=["valor cobrado de IPTU"],
            target_columns=["valor IPTU"],
        )
        assert mapping == {"valor cobrado de IPTU": "valor IPTU"}

    def test_no_mapping_when_target_absent(self):
        mapping = match_and_map_columns(
            source_columns=["quantidade de pavimentos"],
            target_columns=["bairro"],
        )
        assert mapping == {}

    def test_fuzzy_mode_maps_above_threshold(self):
        mapping = match_and_map_columns(
            source_columns=["valor do iptu cobrado"],
            target_columns=["valor IPTU"],
            threshold=0.5,
            only_known_mappings=False,
        )
        assert mapping.get("valor do iptu cobrado") == "valor IPTU"
