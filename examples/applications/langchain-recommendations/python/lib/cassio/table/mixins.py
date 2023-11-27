from operator import itemgetter
import json

from typing import (
    Any,
    cast,
    Dict,
    List,
    Iterable,
    Optional,
    Set,
    Tuple,
    Union,
)

from cassandra.cluster import ResponseFuture  # type: ignore

from cassio.utils.vector.distance_metrics import distance_metrics

from cassio.table.cql import (
    CQLOpType,
    DELETE_CQL_TEMPLATE,
    SELECT_CQL_TEMPLATE,
    CREATE_INDEX_CQL_TEMPLATE,
    # CREATE_KEYS_INDEX_CQL_TEMPLATE,
    CREATE_ENTRIES_INDEX_CQL_TEMPLATE,
    SELECT_ANN_CQL_TEMPLATE,
)
from cassio.table.table_types import (
    ColumnSpecType,
    RowType,
    RowWithDistanceType,
    normalize_type_desc,
    rearrange_pk_type,
    MetadataIndexingMode,
    MetadataIndexingPolicy,
    is_metadata_field_indexed,
)
from cassio.table.base_table import BaseTable


class BaseTableMixin(BaseTable):
    """All other mixins should inherit from this one."""

    pass


class ClusteredMixin(BaseTableMixin):
    def __init__(
        self,
        *pargs: Any,
        partition_id_type: Union[str, List[str]] = ["TEXT"],
        partition_id: Optional[Any] = None,
        ordering_in_partition: str = "ASC",
        **kwargs: Any,
    ) -> None:
        self.partition_id_type = normalize_type_desc(partition_id_type)
        self.partition_id = partition_id
        self.ordering_in_partition = ordering_in_partition.upper()
        super().__init__(*pargs, **kwargs)

    def _schema_pk(self) -> List[ColumnSpecType]:
        assert len(self.partition_id_type) == 1
        return [
            ("partition_id", self.partition_id_type[0]),
        ]

    def _schema_cc(self) -> List[ColumnSpecType]:
        return self._schema_row_id()

    def _extract_where_clause_blocks(
        self, args_dict: Any
    ) -> Tuple[Any, List[str], Tuple[Any, ...]]:
        """
        If a null partition_id arrives to WHERE construction, it is silently
        discarded from the set of conditions to create.
        This enables e.g. ANN vector search across partitions of a clustered table.

        It is the database's responsibility to raise an error if unacceptable.
        """
        if "partition_id" in args_dict and args_dict["partition_id"] is None:
            cleaned_args_dict = {
                k: v for k, v in args_dict.items() if k != "partition_id"
            }
        else:
            cleaned_args_dict = args_dict
        #
        return super()._extract_where_clause_blocks(cleaned_args_dict)

    def _delete_partition(
        self, is_async: bool, partition_id: Optional[str] = None
    ) -> Union[None, ResponseFuture]:
        _partition_id = self.partition_id if partition_id is None else partition_id
        #
        where_clause = "WHERE " + "partition_id = %s"
        delete_cql_vals = (_partition_id,)
        delete_cql = DELETE_CQL_TEMPLATE.format(
            where_clause=where_clause,
        )
        if is_async:
            return self.execute_cql_async(
                delete_cql, args=delete_cql_vals, op_type=CQLOpType.WRITE
            )
        else:
            self.execute_cql(delete_cql, args=delete_cql_vals, op_type=CQLOpType.WRITE)
            return None

    def delete_partition(self, partition_id: Optional[str] = None) -> None:
        self._delete_partition(is_async=False, partition_id=partition_id)
        return None

    def delete_partition_async(
        self, partition_id: Optional[str] = None
    ) -> ResponseFuture:
        return self._delete_partition(is_async=True, partition_id=partition_id)

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        # if partition id provided in call, takes precedence over instance value
        arg_pid = args_dict.get("partition_id")
        instance_pid = self.partition_id
        _partition_id = instance_pid if arg_pid is None else arg_pid
        new_args_dict = {
            **{"partition_id": _partition_id},
            **args_dict,
        }
        return super()._normalize_kwargs(new_args_dict)

    def get_partition(
        self, partition_id: Optional[str] = None, n: Optional[int] = None, **kwargs: Any
    ) -> Iterable[RowType]:
        _partition_id = self.partition_id if partition_id is None else partition_id
        get_p_cql_vals: Tuple[Any, ...] = tuple()
        #
        # TODO: work on a columns: Optional[List[str]] = None
        # (but with nuanced handling of the column-magic we have here)
        columns = None
        if columns is None:
            columns_desc = "*"
        else:
            # TODO: handle translations here?
            # columns_desc = ", ".join(columns)
            raise NotImplementedError("Column selection is not implemented.")
        # WHERE can admit other sources (e.g. medata if the corresponding mixin)
        # so we escalate to standard WHERE-creation route and reinject the partition
        n_kwargs = self._normalize_kwargs(
            {
                **{"partition_id": _partition_id},
                **kwargs,
            }
        )
        (args_dict, wc_blocks, wc_vals) = self._extract_where_clause_blocks(n_kwargs)
        # check for exhaustion:
        assert args_dict == {}
        where_clause = "WHERE " + " AND ".join(wc_blocks)
        where_cql_vals = list(wc_vals)
        #
        if n is None:
            limit_clause = ""
            limit_cql_vals = []
        else:
            limit_clause = "LIMIT %s"
            limit_cql_vals = [n]
        #
        select_cql = SELECT_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        get_p_cql_vals = tuple(where_cql_vals + limit_cql_vals)
        return (
            self._normalize_row(raw_row)
            for raw_row in self.execute_cql(
                select_cql,
                args=get_p_cql_vals,
                op_type=CQLOpType.READ,
            )
        )

    def get_partition_async(
        self, partition_id: Optional[str] = None, n: Optional[int] = None, **kwargs: Any
    ) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")


class MetadataMixin(BaseTableMixin):
    def __init__(
        self,
        *pargs: Any,
        metadata_indexing: Union[Tuple[str, Iterable[str]], str] = "all",
        **kwargs: Any,
    ) -> None:
        self.metadata_indexing_policy = self._normalize_metadata_indexing_policy(
            metadata_indexing
        )
        super().__init__(*pargs, **kwargs)

    @staticmethod
    def _normalize_metadata_indexing_policy(
        metadata_indexing: Union[Tuple[str, Iterable[str]], str]
    ) -> MetadataIndexingPolicy:
        mode: MetadataIndexingMode
        fields: Set[str]
        # metadata indexing policy normalization:
        if isinstance(metadata_indexing, str):
            if metadata_indexing.lower() == "all":
                mode, fields = (MetadataIndexingMode.DEFAULT_TO_SEARCHABLE, set())
            elif metadata_indexing.lower() == "none":
                mode, fields = (MetadataIndexingMode.DEFAULT_TO_UNSEARCHABLE, set())
            else:
                raise ValueError(
                    f"Unsupported metadata_indexing value '{metadata_indexing}'"
                )
        else:
            assert len(metadata_indexing) == 2
            # it's a 2-tuple (mode, fields) still to normalize
            _mode, _field_spec = metadata_indexing
            fields = {_field_spec} if isinstance(_field_spec, str) else set(_field_spec)
            if _mode.lower() in {
                "default_to_unsearchable",
                "allowlist",
                "allow",
                "allow_list",
            }:
                mode = MetadataIndexingMode.DEFAULT_TO_UNSEARCHABLE
            elif _mode.lower() in {
                "default_to_searchable",
                "denylist",
                "deny",
                "deny_list",
            }:
                mode = MetadataIndexingMode.DEFAULT_TO_SEARCHABLE
            else:
                raise ValueError(
                    f"Unsupported metadata indexing mode specification '{_mode}'"
                )
        return (mode, fields)

    def _schema_da(self) -> List[ColumnSpecType]:
        return super()._schema_da() + [
            ("attributes_blob", "TEXT"),
            ("metadata_s", "MAP<TEXT,TEXT>"),
        ]

    def db_setup(self) -> None:
        # Currently: an 'entries' index on the metadata_s column
        super().db_setup()
        #
        entries_index_columns = ["metadata_s"]
        for entries_index_column in entries_index_columns:
            index_name = f"eidx_{entries_index_column}"
            index_column = f"{entries_index_column}"
            create_index_cql = CREATE_ENTRIES_INDEX_CQL_TEMPLATE.format(
                index_name=index_name,
                index_column=index_column,
            )
            self.execute_cql(create_index_cql, op_type=CQLOpType.SCHEMA)
        #
        return

    @staticmethod
    def _serialize_md_dict(md_dict: Dict[str, Any]) -> str:
        return json.dumps(md_dict, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _deserialize_md_dict(md_string: str) -> Dict[str, Any]:
        return cast(Dict[str, Any], json.loads(md_string))

    @staticmethod
    def _coerce_string(value: Any) -> str:
        if isinstance(value, str):
            return value
        elif isinstance(value, bool):
            # bool MUST come before int in this chain of ifs!
            return json.dumps(value)
        elif isinstance(value, int):
            # we don't want to store '1' and '1.0' differently
            # for the sake of metadata-filtered retrieval:
            return json.dumps(float(value))
        elif isinstance(value, float):
            return json.dumps(value)
        elif value is None:
            return json.dumps(value)
        else:
            # when all else fails ...
            return str(value)

    def _split_metadata_fields(self, md_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Split the *indexed* part of the metadata in separate parts,
        one per Cassandra column.

        Currently: everything gets cast to a string and goes to a single table
        column. This means:
            - strings are fine
            - floats and integers v: they are cast to str(v)
            - booleans: 'true'/'false' (JSON style)
            - None => 'null' (JSON style)
            - anything else v => str(v), no questions asked

        Caveat: one gets strings back when reading metadata
        """

        # TODO: more care about types here
        stringy_part = {k: self._coerce_string(v) for k, v in md_dict.items()}
        return {
            "metadata_s": stringy_part,
        }

    def _normalize_row(self, raw_row: Any) -> Dict[str, Any]:
        md_columns_defaults: Dict[str, Any] = {
            "metadata_s": {},
        }
        pre_normalized = super()._normalize_row(raw_row)
        #
        row_rest = {
            k: v
            for k, v in pre_normalized.items()
            if k not in md_columns_defaults
            if k != "attributes_blob"
        }
        mergee_md_fields = {
            k: v for k, v in pre_normalized.items() if k in md_columns_defaults
        }
        normalized_mergee_md_fields = {
            k: v if v is not None else md_columns_defaults[k]
            for k, v in mergee_md_fields.items()
        }
        r_md_from_s = {
            k: v for k, v in normalized_mergee_md_fields["metadata_s"].items()
        }
        #
        raw_attr_blob = pre_normalized.get("attributes_blob")
        if raw_attr_blob is not None:
            r_attrs = self._deserialize_md_dict(raw_attr_blob)
        else:
            r_attrs = {}
        #
        row_metadata = {
            "metadata": {
                **r_attrs,
                **r_md_from_s,
            },
        }
        #
        normalized = {
            **row_metadata,
            **row_rest,
        }
        return normalized

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        _metadata_input_dict = args_dict.get("metadata", {})
        # separate indexed and non-indexed (=attributes) as per indexing policy
        metadata_indexed_dict = {
            k: v
            for k, v in _metadata_input_dict.items()
            if is_metadata_field_indexed(k, self.metadata_indexing_policy)
        }
        attributes_dict = {
            k: self._coerce_string(v)
            for k, v in _metadata_input_dict.items()
            if not is_metadata_field_indexed(k, self.metadata_indexing_policy)
        }
        #
        if attributes_dict != {}:
            attributes_fields = {
                "attributes_blob": self._serialize_md_dict(attributes_dict)
            }
        else:
            attributes_fields = {}
        #
        new_metadata_fields = {
            k: v
            for k, v in self._split_metadata_fields(metadata_indexed_dict).items()
            if v != {} and v != set()
        }
        #
        new_args_dict = {
            **{k: v for k, v in args_dict.items() if k != "metadata"},
            **attributes_fields,
            **new_metadata_fields,
        }
        return super()._normalize_kwargs(new_args_dict)

    def _extract_where_clause_blocks(
        self, args_dict: Any
    ) -> Tuple[Any, List[str], Tuple[Any, ...]]:
        # This always happens after a corresponding _normalize_kwargs,
        # so the metadata, if present, appears as split-fields.
        assert "metadata" not in args_dict
        if "attributes_blob" in args_dict:
            raise ValueError("Non-indexed metadata fields cannot be used in queries.")
        md_keys = {"metadata_s"}
        new_args_dict = {k: v for k, v in args_dict.items() if k not in md_keys}
        # Here the "metadata" entry is made into specific where clauses
        split_metadata = {k: v for k, v in args_dict.items() if k in md_keys}
        these_wc_blocks: List[str] = []
        these_wc_vals_list: List[Any] = []
        # WHERE creation:
        for k, v in sorted(split_metadata.get("metadata_s", {}).items()):
            these_wc_blocks.append(f"metadata_s['{k}'] = %s")
            these_wc_vals_list.append(v)
        # no new kwargs keys are created, all goes to WHERE
        this_args_dict: Dict[str, Any] = {}
        these_wc_vals = tuple(these_wc_vals_list)
        # ready to defer to superclass(es), then collate-and-return
        (s_args_dict, s_wc_blocks, s_wc_vals) = super()._extract_where_clause_blocks(
            new_args_dict
        )
        return (
            {**this_args_dict, **s_args_dict},
            these_wc_blocks + s_wc_blocks,
            tuple(list(these_wc_vals) + list(s_wc_vals)),
        )

    def find_entries(self, n: int, **kwargs: Any) -> Iterable[RowType]:
        columns_desc, where_clause, get_cql_vals = self._parse_select_core_params(
            **kwargs
        )
        limit_clause = "LIMIT %s"
        limit_cql_vals = [n]
        select_vals = tuple(list(get_cql_vals) + limit_cql_vals)
        #
        select_cql = SELECT_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        result_set = self.execute_cql(
            select_cql, args=select_vals, op_type=CQLOpType.READ
        )
        return (self._normalize_row(result) for result in result_set)

    def find_entries_async(self, n: int, **kwargs: Any) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")

    def find_and_delete_entries(
        self, n: Optional[int] = None, batch_size: int = 20, **kwargs: Any
    ) -> int:
        # Use `find_entries` to delete entries based
        # on queries with metadata, etc. Suitable when `find_entries` is a fit.
        # Returns the number of rows supposedly deleted.
        # Warning: reads before writing. Not very efficient (nor Cassandraic).
        #
        # TODO: Use the 'columns' for a narrowed projection
        # TODO: decouple finding and deleting (streaming) for faster performance
        primary_key_cols = [col for col, _ in self._schema_primary_key()]
        #
        visited_tuples: Set[Tuple] = set()
        batch_size = 20
        if n is not None:
            to_delete = min(batch_size, n - len(visited_tuples))
        else:
            to_delete = batch_size
        while to_delete > 0:
            del_pkargs = [
                [found_row[pkc] for pkc in primary_key_cols]
                for found_row in self.find_entries(n=to_delete, **kwargs)
            ]
            if del_pkargs == []:
                break
            d_futures = [
                self.delete_async(
                    **{pkc: pkv for pkc, pkv in zip(primary_key_cols, del_pkarg)}
                )
                for del_pkarg in del_pkargs
                if tuple(del_pkarg) not in visited_tuples
            ]
            if d_futures == []:
                break
            for d_future in d_futures:
                _ = d_future.result()
            visited_tuples = visited_tuples | {
                tuple(del_pkarg) for del_pkarg in del_pkargs
            }
            if n is not None:
                to_delete = min(batch_size, n - len(visited_tuples))
            else:
                to_delete = batch_size
        #
        return len(visited_tuples)

    def find_and_delete_entries_async(self, **kwargs: Any) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")


class VectorMixin(BaseTableMixin):
    def __init__(self, *pargs: Any, vector_dimension: int, **kwargs: Any) -> None:
        self.vector_dimension = vector_dimension
        super().__init__(*pargs, **kwargs)

    def _schema_da(self) -> List[ColumnSpecType]:
        return super()._schema_da() + [
            ("vector", f"VECTOR<FLOAT,{self.vector_dimension}>")
        ]

    def db_setup(self) -> None:
        super().db_setup()
        # index on the vector column:
        index_name = "idx_vector"
        index_column = "vector"
        create_index_cql = CREATE_INDEX_CQL_TEMPLATE.format(
            index_name=index_name,
            index_column=index_column,
        )
        self.execute_cql(create_index_cql, op_type=CQLOpType.SCHEMA)
        return

    def ann_search(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> Iterable[RowType]:
        n_kwargs = self._normalize_kwargs(kwargs)
        # TODO: work on a columns: Optional[List[str]] = None
        # (but with nuanced handling of the column-magic we have here)
        columns = None
        if columns is None:
            columns_desc = "*"
        else:
            # TODO: handle translations here?
            # columns_desc = ", ".join(columns)
            raise NotImplementedError("Column selection is not implemented.")
        #
        if all(x == 0 for x in vector):
            # TODO: lift/relax this constraint when non-cosine metrics are there.
            raise ValueError("Cannot use identically-zero vectors in cos/ANN search.")
        #
        vector_column = "vector"
        vector_cql_vals = [vector]
        #
        (
            rest_kwargs,
            where_clause_blocks,
            where_cql_vals,
        ) = self._extract_where_clause_blocks(n_kwargs)
        assert rest_kwargs == {}
        if where_clause_blocks == []:
            where_clause = ""
        else:
            where_clause = "WHERE " + " AND ".join(where_clause_blocks)
        #
        limit_clause = "LIMIT %s"
        limit_cql_vals = [n]
        #
        select_ann_cql = SELECT_ANN_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            vector_column=vector_column,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        #
        select_ann_cql_vals = tuple(
            list(where_cql_vals) + vector_cql_vals + limit_cql_vals
        )
        result_set = self.execute_cql(
            select_ann_cql, args=select_ann_cql_vals, op_type=CQLOpType.READ
        )
        return (self._normalize_row(result) for result in result_set)

    def ann_search_async(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")

    def metric_ann_search(
        self,
        vector: List[float],
        n: int,
        metric: str,
        metric_threshold: Optional[float] = None,
        **kwargs: Any,
    ) -> Iterable[RowWithDistanceType]:
        rows = list(self.ann_search(vector, n, **kwargs))
        if rows == []:
            return []
        else:
            # sort, cut, validate and prepare for returning
            # evaluate metric
            distance_function, distance_reversed = distance_metrics[metric]
            row_vectors = [row["vector"] for row in rows]
            # enrich with their metric score
            rows_with_metric = list(
                zip(
                    distance_function(row_vectors, vector),
                    rows,
                )
            )
            # sort rows by metric score. First handle metric/threshold
            if metric_threshold is not None:
                _used_thr = metric_threshold
                if distance_reversed:

                    def _thresholder(mtx: float, thr: float) -> bool:
                        return mtx >= thr

                else:

                    def _thresholder(mtx: float, thr: float) -> bool:
                        return mtx <= thr

            else:
                # this to satisfy the type checker
                _used_thr = 0.0
                # no hits are discarded
                def _thresholder(mtx: float, thr: float) -> bool:
                    return True

            #
            sorted_passing_rows = sorted(
                (pair for pair in rows_with_metric if _thresholder(pair[0], _used_thr)),
                key=itemgetter(0),
                reverse=distance_reversed,
            )
            # return a list of hits with their distance (as JSON)
            enriched_hits = (
                {
                    **hit,
                    **{"distance": distance},
                }
                for distance, hit in sorted_passing_rows
            )
            return enriched_hits

    def metric_ann_search_async(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")


class ElasticKeyMixin(BaseTableMixin):
    def __init__(self, *pargs: Any, keys: List[str], **kwargs: Any) -> None:
        if "row_id_type" in kwargs:
            raise ValueError("'row_id_type' not allowed for elastic tables.")
        self.keys = keys
        self.key_desc = self._serialize_key_list(self.keys)
        row_id_type = ["TEXT", "TEXT"]
        new_kwargs = {
            **{"row_id_type": row_id_type},
            **kwargs,
        }
        super().__init__(*pargs, **new_kwargs)

    @staticmethod
    def _serialize_key_list(key_vals: List[Any]) -> str:
        return json.dumps(key_vals, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _deserialize_key_list(keys_str: str) -> List[Any]:
        return cast(List[Any], json.loads(keys_str))

    def _normalize_row(self, raw_row: Any) -> Dict[str, Any]:
        key_fields = {"key_desc", "key_vals"}
        pre_normalized = super()._normalize_row(raw_row)
        row_key = {k: v for k, v in pre_normalized.items() if k in key_fields}
        row_rest = {k: v for k, v in pre_normalized.items() if k not in key_fields}
        if row_key == {}:
            key_dict = {}
        else:
            # unpack the keys
            assert len(row_key) == 2
            assert self._deserialize_key_list(row_key["key_desc"]) == self.keys
            key_dict = {
                k: v
                for k, v in zip(
                    self.keys,
                    self._deserialize_key_list(row_key["key_vals"]),
                )
            }
        return {
            **key_dict,
            **row_rest,
        }

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        # transform provided "keys" into the elastic-representation two-val form
        key_args = {k: v for k, v in args_dict.items() if k in self.keys}
        # the "key" is passed all-or-nothing:
        assert set(key_args.keys()) == set(self.keys) or key_args == {}
        if key_args != {}:
            key_vals = self._serialize_key_list(
                [key_args[key_col] for key_col in self.keys]
            )
            #
            key_args_dict = {
                "key_vals": key_vals,
                "key_desc": self.key_desc,
            }
            other_args_dict = {k: v for k, v in args_dict.items() if k not in self.keys}
            new_args_dict = {
                **key_args_dict,
                **other_args_dict,
            }
        else:
            new_args_dict = args_dict
        return super()._normalize_kwargs(new_args_dict)

    @staticmethod
    def _schema_row_id() -> List[ColumnSpecType]:
        return [
            ("key_desc", "TEXT"),
            ("key_vals", "TEXT"),
        ]


class TypeNormalizerMixin(BaseTableMixin):

    clustered: bool = False
    elastic: bool = False

    def __init__(self, *pargs: Any, **kwargs: Any) -> None:
        if "primary_key_type" in kwargs:
            pk_arg = kwargs["primary_key_type"]
            num_elastic_keys = len(kwargs["keys"]) if self.elastic else None
            col_type_map = rearrange_pk_type(pk_arg, self.clustered, num_elastic_keys)
            new_kwargs = {
                **col_type_map,
                **{k: v for k, v in kwargs.items() if k != "primary_key_type"},
            }
        else:
            new_kwargs = kwargs
        super().__init__(*pargs, **new_kwargs)
