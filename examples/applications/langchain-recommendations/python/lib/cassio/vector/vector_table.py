"""
Compatibility layer for legacy VectorTable (used by LangChain integration
(as of August 2023).

Note: This is to be replaced by direct usage of the table-class-hierarchy classes.
"""

from warnings import warn
from typing import List, Dict, Any, Optional

from cassandra.cluster import ResponseFuture  # type: ignore

from cassio.table.table_types import RowType
from cassio.table.tables import (
    MetadataVectorCassandraTable,
)

new_columns_to_legacy = {
    "row_id": "document_id",
    "body_blob": "document",
    "vector": "embedding_vector",
}
legacy_columns_to_new = {v: k for k, v in new_columns_to_legacy.items()}


class VectorTable:
    """
    This class is a rewriting of the VectorTable created for use in LangChain
    integration, this time relying on the class-table-hierarchy (cassio.table.*).

    It mostly provides a translation layer between parameters and key names,
    using a metadata+vector table class internally.

    Additional kwargs, for use in this new table class, are passed as they are
    in order to enable their usage already before adapting the LangChain
    integration code.
    """

    DEPRECATION_MESSAGE = (
        "Class `VectorTable` is a legacy construct and "
        "will be deprecated in future versions of CassIO."
    )

    def __init__(self, *pargs: Any, **kwargs: Dict[str, Any]):
        #
        warn(self.DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
        #
        if "embedding_dimension" in kwargs:
            vector_dimension = kwargs["embedding_dimension"]
            new_kwargs = {
                **{
                    k: v
                    for k, v in kwargs.items()
                    if k != "embedding_dimension"
                    # let's get rid of the infamous 'auto_id' here:
                    if k != "auto_id"
                },
                **{"vector_dimension": vector_dimension},
            }
        else:
            new_kwargs = kwargs
        # this legacy VectorTable will have everything indexed for search:
        md_kwargs = {
            **{"metadata_indexing": "all"},
            **new_kwargs,
        }
        #
        self.table = MetadataVectorCassandraTable(*pargs, **md_kwargs)

    def search(
        self,
        embedding_vector: List[float],
        top_k: int,
        metric: str = "cos",
        metric_threshold: Optional[float] = None,
        **kwargs: Any,
    ) -> List[RowType]:
        # get rows by ANN
        enriched_hits = self.table.metric_ann_search(
            vector=embedding_vector,
            n=top_k,
            metric=metric,
            metric_threshold=metric_threshold,
            **kwargs,
        )
        #
        return [self._make_dict_legacy(rich_hit) for rich_hit in enriched_hits]

    def put(
        self,
        document: str,
        embedding_vector: List[float],
        document_id: Any,
        metadata: Dict[str, Any] = {},
        ttl_seconds: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        self.table.put(
            row_id=document_id,
            body_blob=document,
            vector=embedding_vector,
            metadata=metadata or {},
            ttl_seconds=ttl_seconds,
            **kwargs,
        )

    def put_async(
        self,
        document: str,
        embedding_vector: List[float],
        document_id: Any,
        metadata: Dict[str, Any],
        ttl_seconds: int,
        **kwargs: Any,
    ) -> ResponseFuture:
        return self.table.put_async(
            row_id=document_id,
            body_blob=document,
            vector=embedding_vector,
            metadata=metadata or {},
            ttl_seconds=ttl_seconds,
            **kwargs,
        )

    def get(self, document_id: Any, **kwargs: Any) -> Optional[RowType]:
        row_or_none = self.table.get(row_id=document_id, **kwargs)
        if row_or_none:
            return self._make_dict_legacy(row_or_none)
        else:
            return row_or_none

    def delete(self, document_id: Any, **kwargs: Any) -> None:
        self.table.delete(row_id=document_id, **kwargs)
        return None

    def clear(self) -> None:
        self.table.clear()
        return None

    @staticmethod
    def _make_dict_legacy(new_dict: RowType) -> RowType:
        return {new_columns_to_legacy.get(k, k): v for k, v in new_dict.items()}
