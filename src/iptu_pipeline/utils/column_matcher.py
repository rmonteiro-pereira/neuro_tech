"""
Column name fuzzy matching utility.
Matches columns with similar names for schema alignment.
"""
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher

from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("column_matcher")


def similarity_score(str1: str, str2: str) -> float:
    """
    Calculate similarity score between two strings.
    
    Args:
        str1: First string
        str2: Second string
    
    Returns:
        Similarity score between 0 and 1
    """
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()


def find_similar_columns(
    source_columns: List[str],
    target_columns: List[str],
    threshold: float = 0.7,
    known_mappings: Optional[Dict[str, str]] = None
) -> Dict[str, Tuple[str, float]]:
    """
    Find similar columns between source and target using fuzzy matching.
    
    Args:
        source_columns: List of source column names
        target_columns: List of target column names
        threshold: Minimum similarity score (0-1)
        known_mappings: Known column mappings to prioritize
    
    Returns:
        Dictionary mapping source_column -> (target_column, similarity_score)
    """
    matches = {}
    
    # Normalize column names for comparison
    source_normalized = {col.lower().strip(): col for col in source_columns}
    target_normalized = {col.lower().strip(): col for col in target_columns}
    target_set = set(target_normalized.keys())
    
    # First, check known mappings (exact matches with different case/spacing)
    if known_mappings:
        for source_col, target_col in known_mappings.items():
            source_norm = source_col.lower().strip()
            target_norm = target_col.lower().strip()
            
            if source_norm in source_normalized and target_norm in target_set:
                matches[source_normalized[source_norm]] = (
                    target_normalized[target_norm],
                    1.0  # Perfect match for known mappings
                )
    
    # Then, fuzzy match remaining columns
    for source_col in source_columns:
        if source_col in matches:
            continue  # Already matched
        
        source_norm = source_col.lower().strip()
        
        # Skip if exact match exists
        if source_norm in target_set:
            continue
        
        best_match = None
        best_score = 0.0
        
        for target_col in target_columns:
            target_norm = target_col.lower().strip()
            
            # Skip if already in matches (as target)
            if any(tgt == target_col for _, (tgt, _) in matches.items()):
                continue
            
            score = similarity_score(source_col, target_col)
            
            if score > best_score and score >= threshold:
                best_score = score
                best_match = target_col
        
        if best_match:
            matches[source_col] = (best_match, best_score)
    
    return matches


# Known column mappings for IPTU data - these are the specific columns we want to match
KNOWN_COLUMN_MAPPINGS = {
    "quantidade de pavimentos": "quant pavimentos",
    "valor cobrado de IPTU": "valor IPTU",
}


def match_and_map_columns(
    source_columns: List[str],
    target_columns: List[str],
    threshold: float = 0.7,
    only_known_mappings: bool = True
) -> Dict[str, str]:
    """
    Match and map similar columns from source to target.
    
    Args:
        source_columns: Source column names
        target_columns: Target column names
        threshold: Similarity threshold (only used if only_known_mappings=False)
        only_known_mappings: If True, only match columns from KNOWN_COLUMN_MAPPINGS
    
    Returns:
        Dictionary mapping source_column -> target_column
    """
    matches = {}
    
    # Normalize for comparison
    source_normalized = {col.lower().strip(): col for col in source_columns}
    target_normalized = {col.lower().strip(): col for col in target_columns}
    target_set = set(target_normalized.keys())
    
    if only_known_mappings:
        # Only match known mappings - these are the specific columns we want to join
        for known_source, known_target in KNOWN_COLUMN_MAPPINGS.items():
            source_norm = known_source.lower().strip()
            target_norm = known_target.lower().strip()
            
            # Check if source column exists (with case variations)
            matching_source = None
            for norm, orig in source_normalized.items():
                # Use high threshold for known mappings
                if norm == source_norm or similarity_score(norm, source_norm) >= 0.9:
                    matching_source = orig
                    break
            
            # Check if target column is in the required columns list
            if matching_source and target_norm in target_set:
                # Verify this is actually a matching scenario
                if matching_source in source_columns and target_normalized[target_norm] in target_columns:
                    matches[matching_source] = target_normalized[target_norm]
    else:
        # Use full fuzzy matching (includes known mappings + others above threshold)
        matches_with_scores = find_similar_columns(
            source_columns,
            target_columns,
            threshold=threshold,
            known_mappings=KNOWN_COLUMN_MAPPINGS
        )
        
        # Convert to simple mapping
        matches = {src: tgt for src, (tgt, score) in matches_with_scores.items()}
    
    return matches