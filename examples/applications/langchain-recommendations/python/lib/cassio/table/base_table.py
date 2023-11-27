from typing import Any, cast, Dict, List, Iterable, Optional, Set, Tuple, Union

from cassandra.query import SimpleStatement, PreparedStatement  # type: ignore
from cassandra.cluster import ResultSet  # type: ignore
from cassandra.cluster import ResponseFuture  # type: ignore

from cassio.config import check_resolve_session, check_resolve_keyspace
from cassio.table.table_types import (
    ColumnSpecType,
    RowType,
    SessionType,
    normalize_type_desc,
)
from cassio.table.cql import (
    CQLOpType,
    CREATE_TABLE_CQL_TEMPLATE,
    TRUNCATE_TABLE_CQL_TEMPLATE,
    DELETE_CQL_TEMPLATE,
    SELECT_CQL_TEMPLATE,
    INSERT_ROW_CQL_TEMPLATE,
)


class BaseTable:

    ordering_in_partition: Optional[str] = None

    def __init__(
        self,
        table: str,
        session: Optional[SessionType] = None,
        keyspace: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        row_id_type: Union[str, List[str]] = ["TEXT"],
        skip_provisioning: bool = False,
    ) -> None:
        self.session = check_resolve_session(session)
        self.keyspace = check_resolve_keyspace(keyspace)
        self.table = table
        self.ttl_seconds = ttl_seconds
        self.row_id_type = normalize_type_desc(row_id_type)
        self.skip_provisioning = skip_provisioning
        self._prepared_statements: Dict[str, PreparedStatement] = {}
        self.db_setup()

    def _schema_row_id(self) -> List[ColumnSpecType]:
        assert len(self.row_id_type) == 1
        return [
            ("row_id", self.row_id_type[0]),
        ]

    def _schema_pk(self) -> List[ColumnSpecType]:
        return self._schema_row_id()

    def _schema_cc(self) -> List[ColumnSpecType]:
        return []

    def _schema_da(self) -> List[ColumnSpecType]:
        return [
            ("body_blob", "TEXT"),
        ]

    def _schema(self) -> Dict[str, List[ColumnSpecType]]:
        return {
            "pk": self._schema_pk(),
            "cc": self._schema_cc(),
            "da": self._schema_da(),
        }

    def _schema_primary_key(self) -> List[ColumnSpecType]:
        return self._schema_pk() + self._schema_cc()

    def _schema_collist(self) -> List[ColumnSpecType]:
        full_list = self._schema_da() + self._schema_cc() + self._schema_pk()
        return full_list

    def _schema_colnameset(self) -> Set[str]:
        full_list = self._schema_collist()
        full_set = set(col for col, _ in full_list)
        assert len(full_list) == len(full_set)
        return full_set

    def _desc_table(self) -> str:
        columns = self._schema()
        col_str = (
            "[("
            + ", ".join("%s(%s)" % colspec for colspec in columns["pk"])
            + ") "
            + ", ".join("%s(%s)" % colspec for colspec in columns["cc"])
            + "] "
            + ", ".join("%s(%s)" % colspec for colspec in columns["da"])
        )
        return col_str

    def _extract_where_clause_blocks(
        self, args_dict: Any
    ) -> Tuple[Any, List[str], Tuple[Any, ...]]:
        # Removes some of the passed kwargs and returns the remaining,
        # plus the pieces for a WHERE
        _allowed_colspecs = self._schema_collist()
        passed_columns = sorted(
            [col for col, _ in _allowed_colspecs if col in args_dict]
        )
        residual_args = {k: v for k, v in args_dict.items() if k not in passed_columns}
        where_clause_blocks = [f"{col} = %s" for col in passed_columns]
        where_clause_vals = tuple([args_dict[col] for col in passed_columns])
        return (
            residual_args,
            where_clause_blocks,
            where_clause_vals,
        )

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        return args_dict

    def _normalize_row(self, raw_row: Any) -> Dict[str, Any]:
        if isinstance(raw_row, dict):
            dict_row = raw_row
        else:
            dict_row = raw_row._asdict()
        #
        return dict_row

    def _delete(
        self, is_async: bool, **kwargs: Dict[str, Any]
    ) -> Union[None, ResponseFuture]:
        n_kwargs = self._normalize_kwargs(kwargs)
        (
            rest_kwargs,
            where_clause_blocks,
            delete_cql_vals,
        ) = self._extract_where_clause_blocks(n_kwargs)
        assert rest_kwargs == {}
        where_clause = "WHERE " + " AND ".join(where_clause_blocks)
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

    def delete(self, **kwargs: Dict[str, Any]) -> None:
        self._delete(is_async=False, **kwargs)
        return None

    def delete_async(self, **kwargs: Dict[str, Any]) -> ResponseFuture:
        return self._delete(is_async=True, **kwargs)

    def _clear(self, is_async: bool) -> Union[None, ResponseFuture]:
        truncate_table_cql = TRUNCATE_TABLE_CQL_TEMPLATE.format()
        if is_async:
            return self.execute_cql_async(
                truncate_table_cql, args=tuple(), op_type=CQLOpType.WRITE
            )
        else:
            self.execute_cql(truncate_table_cql, args=tuple(), op_type=CQLOpType.WRITE)
            return None

    def clear(self) -> None:
        self._clear(is_async=False)
        return None

    def clear_async(self) -> ResponseFuture:
        return self._clear(is_async=True)

    def _parse_select_core_params(
        self, **kwargs: Dict[str, Any]
    ) -> Tuple[str, str, Tuple[Any, ...]]:
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
        (
            rest_kwargs,
            where_clause_blocks,
            select_cql_vals,
        ) = self._extract_where_clause_blocks(n_kwargs)
        assert rest_kwargs == {}
        where_clause = "WHERE " + " AND ".join(where_clause_blocks)
        return columns_desc, where_clause, select_cql_vals

    def get(self, **kwargs: Any) -> Optional[RowType]:
        columns_desc, where_clause, get_cql_vals = self._parse_select_core_params(
            **kwargs
        )
        limit_clause = ""
        limit_cql_vals: List[Any] = []
        select_vals = tuple(list(get_cql_vals) + limit_cql_vals)
        #
        select_cql = SELECT_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        # dancing around the result set (to comply with type checking):
        result_set = self.execute_cql(
            select_cql, args=select_vals, op_type=CQLOpType.READ
        )
        if isinstance(result_set, ResultSet):
            result = result_set.one()
        else:
            result = None
        #
        if result is None:
            return result
        else:
            return self._normalize_row(result)

    def get_async(self, **kwargs: Dict[str, Any]) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")

    def _put(
        self, is_async: bool, **kwargs: Dict[str, Any]
    ) -> Union[None, ResponseFuture]:
        n_kwargs = self._normalize_kwargs(kwargs)
        primary_key = self._schema_primary_key()
        assert set(col for col, _ in primary_key) - set(n_kwargs.keys()) == set()
        columns = [col for col, _ in self._schema_collist() if col in n_kwargs]
        columns_desc = ", ".join(columns)
        insert_cql_vals = [n_kwargs[col] for col in columns]
        value_placeholders = ", ".join("%s" for _ in columns)
        #
        ttl_seconds = (
            n_kwargs["ttl_seconds"] if "ttl_seconds" in n_kwargs else self.ttl_seconds
        )
        if ttl_seconds is not None:
            ttl_spec = "USING TTL %s"
            ttl_vals = [ttl_seconds]
        else:
            ttl_spec = ""
            ttl_vals = []
        #
        insert_cql_args = tuple(insert_cql_vals + ttl_vals)
        insert_cql = INSERT_ROW_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            value_placeholders=value_placeholders,
            ttl_spec=ttl_spec,
        )
        #
        if is_async:
            return self.execute_cql_async(
                insert_cql, args=insert_cql_args, op_type=CQLOpType.WRITE
            )
        else:
            self.execute_cql(insert_cql, args=insert_cql_args, op_type=CQLOpType.WRITE)
            return None

    def put(self, **kwargs: Any) -> None:
        self._put(is_async=False, **kwargs)
        return None

    def put_async(self, **kwargs: Any) -> ResponseFuture:
        return self._put(is_async=True, **kwargs)

    def db_setup(self) -> None:
        _schema = self._schema()
        column_specs = [
            f"{col_spec[0]} {col_spec[1]}"
            for _schema_grp in ["pk", "cc", "da"]
            for col_spec in _schema[_schema_grp]
        ]
        pk_spec = ", ".join(col for col, _ in _schema["pk"])
        cc_spec = ", ".join(col for col, _ in _schema["cc"])
        primkey_spec = f"( ( {pk_spec} ) {',' if _schema['cc'] else ''} {cc_spec} )"
        if _schema["cc"]:
            clu_core = ", ".join(
                f"{col} {self.ordering_in_partition}" for col, _ in _schema["cc"]
            )
            clustering_spec = f"WITH CLUSTERING ORDER BY ({clu_core})"
        else:
            clustering_spec = ""
        #
        create_table_cql = CREATE_TABLE_CQL_TEMPLATE.format(
            columns_spec=" ".join(f"  {cs}," for cs in column_specs),
            primkey_spec=primkey_spec,
            clustering_spec=clustering_spec,
        )
        self.execute_cql(create_table_cql, op_type=CQLOpType.SCHEMA)

    def _finalize_cql_semitemplate(self, cql_semitemplate: str) -> str:
        table_fqname = f"{self.keyspace}.{self.table}"
        table_name = self.table
        final_cql = cql_semitemplate.format(
            table_fqname=table_fqname, table_name=table_name
        )
        return final_cql

    def _obtain_prepared_statement(self, final_cql: str) -> PreparedStatement:
        # TODO: improve this placeholder handling
        _preparable_cql = final_cql.replace("%s", "?")
        # handle the cache of prepared statements
        if _preparable_cql not in self._prepared_statements:
            self._prepared_statements[_preparable_cql] = self.session.prepare(
                _preparable_cql
            )
        return self._prepared_statements[_preparable_cql]

    def execute_cql(
        self,
        cql_semitemplate: str,
        op_type: CQLOpType,
        args: Tuple[Any, ...] = tuple(),
    ) -> Iterable[RowType]:
        final_cql = self._finalize_cql_semitemplate(cql_semitemplate)
        #
        if op_type == CQLOpType.SCHEMA and self.skip_provisioning:
            # these operations are not executed for this instance:
            return []
        else:
            if op_type == CQLOpType.SCHEMA:
                # schema operations are not to be 'prepared'
                statement = SimpleStatement(final_cql)
            else:
                statement = self._obtain_prepared_statement(final_cql)
            #
            return cast(Iterable[RowType], self.session.execute(statement, args))

    def execute_cql_async(
        self,
        cql_semitemplate: str,
        op_type: CQLOpType,
        args: Tuple[Any, ...] = tuple(),
    ) -> ResponseFuture:
        final_cql = self._finalize_cql_semitemplate(cql_semitemplate)
        #
        if op_type == CQLOpType.SCHEMA:
            raise RuntimeError("Schema operations cannot be asynchronous")
        else:
            statement = self._obtain_prepared_statement(final_cql)
            #
            return self.session.execute_async(statement, args)
