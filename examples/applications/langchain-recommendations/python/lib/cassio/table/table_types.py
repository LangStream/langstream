from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

ColumnSpecType = Tuple[str, str]
RowType = Dict[str, Any]
RowWithDistanceType = Dict[str, Any]
SessionType = Any


class MetadataIndexingMode(Enum):
    DEFAULT_TO_UNSEARCHABLE = 1
    DEFAULT_TO_SEARCHABLE = 2


MetadataIndexingPolicy = Tuple[MetadataIndexingMode, Set[str]]


def is_metadata_field_indexed(field_name: str, policy: MetadataIndexingPolicy) -> bool:
    p_mode, p_fields = policy
    if p_mode == MetadataIndexingMode.DEFAULT_TO_UNSEARCHABLE:
        return field_name in p_fields
    elif p_mode == MetadataIndexingMode.DEFAULT_TO_SEARCHABLE:
        return field_name not in p_fields
    else:
        raise ValueError(f"Unexpected metadata indexing mode {p_mode}")


def normalize_type_desc(type_desc: Union[str, List[str]]) -> List[str]:
    if isinstance(type_desc, str):
        return [type_desc]
    else:
        return type_desc


def rearrange_pk_type(
    pk_type: Union[str, List[str]],
    clustered: bool = False,
    num_elastic_keys: Optional[int] = None,
) -> Dict[str, List[str]]:
    """A compatibility layer with the 'primary_key_type' specifier on init."""
    _pk_type = normalize_type_desc(pk_type)
    if clustered:
        pk_type, rest_type = _pk_type[0:1], _pk_type[1:]
        if num_elastic_keys:
            assert len(rest_type) == num_elastic_keys
            return {
                "partition_id_type": pk_type,
            }
        else:
            return {
                "row_id_type": rest_type,
                "partition_id_type": pk_type,
            }
    else:
        if num_elastic_keys:
            assert len(_pk_type) == num_elastic_keys
            return {}
        else:
            return {
                "row_id_type": _pk_type,
            }
