#!/usr/bin/env python3
import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import jaydebeapi

# ----------------------------
# Helpers
# ----------------------------
def norm(s) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    return "" if s.lower() in ("none", "null", "nan") else s


def safe_ident(name: str) -> str:
    """
    Conservative identifier cleaning. Keeps underscores and alphanumerics.
    If your DB allows special chars, relax this.
    """
    name = norm(name)
    if not name:
        return name
    return re.sub(r"[^A-Za-z0-9_#$]", "_", name)


def read_table_list_file(path: str) -> List[Tuple[str, str]]:
    """
    Reads /tmp file line by line.

    Supported formats per line:
      1) table_name
      2) schema.table_name
      3) schema,table
      4) schema|table

    Returns list of (schema, table). If schema missing -> "".
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tables file not found: {path}")

    out: List[Tuple[str, str]] = []
    with p.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            schema = ""
            table = ""

            if "," in line:
                parts = [x.strip() for x in line.split(",", 1)]
                schema, table = (parts[0], parts[1]) if len(parts) == 2 else ("", parts[0])
            elif "|" in line:
                parts = [x.strip() for x in line.split("|", 1)]
                schema, table = (parts[0], parts[1]) if len(parts) == 2 else ("", parts[0])
            elif "." in line:
                parts = [x.strip() for x in line.split(".", 1)]
                schema, table = (parts[0], parts[1]) if len(parts) == 2 else ("", parts[0])
            else:
                table = line

            out.append((safe_ident(schema), safe_ident(table)))

    if not out:
        raise ValueError(f"No valid table lines found in: {path}")
    return out


# ----------------------------
# JDBC metadata extraction
# ----------------------------
@dataclass
class ColumnInfo:
    name: str
    jdbc_type: int
    type_name: str
    size: int
    scale: int
    nullable: bool
    default_value: str


@dataclass
class PKInfo:
    name: str
    columns: List[str]  # ordered


@dataclass
class IndexInfo:
    name: str
    unique: bool
    columns: List[str]  # ordered


@dataclass
class FKInfo:
    name: str
    fk_columns: List[str]
    pk_table_schema: str
    pk_table: str
    pk_columns: List[str]


def _rs_to_dicts(rs) -> List[Dict]:
    """
    Convert a java.sql.ResultSet into list of dicts.
    Works because jaydebeapi exposes the underlying Java connection as conn.jconn.
    """
    md = rs.getMetaData()
    col_count = md.getColumnCount()
    col_names = [md.getColumnLabel(i) for i in range(1, col_count + 1)]

    rows = []
    while rs.next():
        row = {}
        for i, cname in enumerate(col_names, start=1):
            row[cname] = rs.getObject(i)
        rows.append(row)
    return rows


def jdbc_connect(db_type: str, host: str, port: str, database: str, username: str, password: str,
                 jars: List[str]):

    db_type = norm(db_type).lower()
    host = norm(host)
    port = norm(port)
    database = norm(database)
    username = norm(username)
    password = norm(password)

    if db_type in ("sqlserver", "mssql", "ms_sql", "microsoft sql server"):
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        # If database is blank, omit databaseName
        if database:
            url = f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
        else:
            url = f"jdbc:sqlserver://{host}:{port};encrypt=true;trustServerCertificate=true"
    elif db_type in ("oracle",):
        driver = "oracle.jdbc.OracleDriver"
        # database here should be SERVICE_NAME (preferred) or SID depending on your env
        # Using service name format:
        url = f"jdbc:oracle:thin:@//{host}:{port}/{database}"
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")

    jars = [norm(x) for x in jars if norm(x)]
    for j in jars:
        if not Path(j).exists():
            raise FileNotFoundError(f"JDBC jar not found: {j}")

    conn = jaydebeapi.connect(driver, url, [username, password], jars)
    return conn


def fetch_table_metadata(conn, src_db_type: str, schema: str, table: str):
    """
    Uses DatabaseMetaData to fetch:
      - columns
      - primary key
      - indexes
      - foreign keys (imported keys)
    """
    jconn = conn.jconn
    meta = jconn.getMetaData()

    # Many DBs are case-sensitive in metadata filters; try a few patterns.
    schema_in = schema if schema else None

    # Columns
    rs_cols = meta.getColumns(None, schema_in, table, None)
    col_rows = _rs_to_dicts(rs_cols)

    columns: List[ColumnInfo] = []
    for r in col_rows:
        columns.append(
            ColumnInfo(
                name=safe_ident(r.get("COLUMN_NAME")),
                jdbc_type=int(r.get("DATA_TYPE")) if r.get("DATA_TYPE") is not None else 0,
                type_name=norm(r.get("TYPE_NAME")),
                size=int(r.get("COLUMN_SIZE")) if r.get("COLUMN_SIZE") is not None else 0,
                scale=int(r.get("DECIMAL_DIGITS")) if r.get("DECIMAL_DIGITS") is not None else 0,
                nullable=(int(r.get("NULLABLE")) == 1) if r.get("NULLABLE") is not None else True,
                default_value=norm(r.get("COLUMN_DEF")),
            )
        )

    if not columns:
        raise RuntimeError(f"No columns found for {schema}.{table} (schema='{schema}')")

    # Primary key
    rs_pk = meta.getPrimaryKeys(None, schema_in, table)
    pk_rows = _rs_to_dicts(rs_pk)
    pk_rows_sorted = sorted(pk_rows, key=lambda x: int(x.get("KEY_SEQ") or 0))

    pk_name = norm(pk_rows_sorted[0].get("PK_NAME")) if pk_rows_sorted else ""
    pk_cols = [safe_ident(r.get("COLUMN_NAME")) for r in pk_rows_sorted] if pk_rows_sorted else []
    pk = PKInfo(name=safe_ident(pk_name) if pk_name else "", columns=pk_cols)

    # Indexes
    # getIndexInfo(catalog, schema, table, unique, approximate)
    rs_idx = meta.getIndexInfo(None, schema_in, table, False, False)
    idx_rows = _rs_to_dicts(rs_idx)

    # Group by index name and order by ORDINAL_POSITION
    idx_map: Dict[str, Dict] = {}
    for r in idx_rows:
        idx_name = norm(r.get("INDEX_NAME"))
        col_name = norm(r.get("COLUMN_NAME"))
        if not idx_name or not col_name:
            continue
        idx_name = safe_ident(idx_name)
        col_name = safe_ident(col_name)
        ord_pos = int(r.get("ORDINAL_POSITION") or 0)
        non_unique = bool(r.get("NON_UNIQUE"))  # True => not unique
        unique = not non_unique

        if idx_name not in idx_map:
            idx_map[idx_name] = {"unique": unique, "cols": []}
        idx_map[idx_name]["cols"].append((ord_pos, col_name))

    indexes: List[IndexInfo] = []
    for idx_name, v in idx_map.items():
        cols_sorted = [c for _, c in sorted(v["cols"], key=lambda x: x[0])]
        # Avoid duplicating PK index (many DBs expose PK as index)
        if pk.columns and set(cols_sorted) == set(pk.columns):
            continue
        indexes.append(IndexInfo(name=idx_name, unique=v["unique"], columns=cols_sorted))

    # Foreign keys (imported keys for this table)
    rs_fk = meta.getImportedKeys(None, schema_in, table)
    fk_rows = _rs_to_dicts(rs_fk)

    # Group FKs by FK_NAME and KEY_SEQ
    fk_map: Dict[str, Dict] = {}
    for r in fk_rows:
        fk_name = safe_ident(norm(r.get("FK_NAME")) or f"FK_{table}")
        key_seq = int(r.get("KEY_SEQ") or 0)
        fk_col = safe_ident(norm(r.get("FKCOLUMN_NAME")))
        pk_col = safe_ident(norm(r.get("PKCOLUMN_NAME")))
        pk_tab = safe_ident(norm(r.get("PKTABLE_NAME")))
        pk_sch = safe_ident(norm(r.get("PKTABLE_SCHEM")))

        if fk_name not in fk_map:
            fk_map[fk_name] = {
                "pk_schema": pk_sch,
                "pk_table": pk_tab,
                "pairs": []
            }
        fk_map[fk_name]["pairs"].append((key_seq, fk_col, pk_col))

    fks: List[FKInfo] = []
    for fk_name, v in fk_map.items():
        pairs = sorted(v["pairs"], key=lambda x: x[0])
        fk_cols = [p[1] for p in pairs]
        pk_cols = [p[2] for p in pairs]
        fks.append(
            FKInfo(
                name=fk_name,
                fk_columns=fk_cols,
                pk_table_schema=v["pk_schema"],
                pk_table=v["pk_table"],
                pk_columns=pk_cols
            )
        )

    return columns, pk, indexes, fks


# ----------------------------
# Type mapping
# ----------------------------
def map_type_sqlserver_to_oracle(col: ColumnInfo) -> str:
    """
    Map SQL Server types -> Oracle
    """
    t = col.type_name.lower()

    # Character
    if "nvarchar" in t or "nchar" in t:
        size = col.size if col.size > 0 else 255
        return f"NVARCHAR2({min(size, 4000)})"
    if "varchar" in t or "char" in t:
        size = col.size if col.size > 0 else 255
        return f"VARCHAR2({min(size, 4000)})"
    if "text" in t or "ntext" in t:
        return "CLOB"

    # Numeric
    if "bigint" in t:
        return "NUMBER(19)"
    if t in ("int",):
        return "NUMBER(10)"
    if "smallint" in t:
        return "NUMBER(5)"
    if "tinyint" in t:
        return "NUMBER(3)"
    if "bit" in t:
        return "NUMBER(1)"

    if "decimal" in t or "numeric" in t:
        p = col.size if col.size > 0 else 38
        s = col.scale if col.scale >= 0 else 0
        return f"NUMBER({min(p,38)},{max(s,0)})"
    if "money" in t or "smallmoney" in t:
        return "NUMBER(19,4)"
    if "float" in t:
        return "BINARY_DOUBLE"
    if "real" in t:
        return "BINARY_FLOAT"

    # Date/time
    if "datetimeoffset" in t:
        return "TIMESTAMP WITH TIME ZONE"
    if "datetime2" in t or "datetime" in t or "smalldatetime" in t:
        return "TIMESTAMP"
    if t == "date":
        return "DATE"
    if t == "time":
        return "TIMESTAMP"

    # Binary
    if "varbinary" in t or "binary" in t or "image" in t:
        return "BLOB"

    # Default fallback
    return "VARCHAR2(4000)"


def map_type_oracle_to_sqlserver(col: ColumnInfo) -> str:
    """
    Map Oracle types -> SQL Server
    """
    t = col.type_name.lower()

    # Character
    if "nvarchar2" in t or t == "nvarchar2":
        size = col.size if col.size > 0 else 255
        return f"NVARCHAR({min(size, 4000)})"
    if "varchar2" in t or t == "varchar2":
        size = col.size if col.size > 0 else 255
        return f"VARCHAR({min(size, 8000)})"
    if t in ("char", "nchar"):
        size = col.size if col.size > 0 else 1
        return f"{t.upper()}({size})"
    if "clob" in t or "nclob" in t:
        return "NVARCHAR(MAX)"

    # Numeric
    if "number" in t:
        p = col.size if col.size > 0 else 38
        s = col.scale if col.scale >= 0 else 0
        # Heuristic for integers:
        if s == 0:
            if p <= 3:
                return "TINYINT"
            if p <= 5:
                return "SMALLINT"
            if p <= 10:
                return "INT"
            if p <= 19:
                return "BIGINT"
        return f"DECIMAL({min(p,38)},{max(s,0)})"

    if "binary_float" in t:
        return "REAL"
    if "binary_double" in t:
        return "FLOAT"

    # Date/time
    if "timestamp" in t:
        # If time zone is present, map to datetimeoffset
        if "with time zone" in t:
            return "DATETIMEOFFSET"
        return "DATETIME2"
    if t == "date":
        return "DATE"

    # Binary
    if "blob" in t or "raw" in t or "long raw" in t:
        return "VARBINARY(MAX)"

    # Default fallback
    return "NVARCHAR(4000)"


def map_type(src_db_type: str, tgt_db_type: str, col: ColumnInfo) -> str:
    s = norm(src_db_type).lower()
    t = norm(tgt_db_type).lower()
    if s.startswith("sql") and t == "oracle":
        return map_type_sqlserver_to_oracle(col)
    if s == "oracle" and t.startswith("sql"):
        return map_type_oracle_to_sqlserver(col)
    raise ValueError(f"Unsupported conversion: {src_db_type} -> {tgt_db_type}")


# ----------------------------
# DDL generators
# ----------------------------
def ddl_create_table(target_db_type: str, target_schema: str, target_table: str,
                     columns: List[ColumnInfo], pk: PKInfo,
                     src_db_type: str) -> str:
    tgt_db = norm(target_db_type).lower()
    ts = safe_ident(target_schema)
    tt = safe_ident(target_table)

    full_name = f"{ts}.{tt}" if ts else tt

    col_lines = []
    for c in columns:
        tgt_type = map_type(src_db_type, target_db_type, c)
        null_sql = "NULL" if c.nullable else "NOT NULL"
        # default handling: keep simple defaults only; complex expressions differ across DBs
        default_sql = ""
        if c.default_value:
            # Safe simple literal defaults (customize if needed)
            dv = c.default_value.strip()
            if dv and len(dv) < 200:
                default_sql = f" DEFAULT {dv}"
        col_lines.append(f"  {safe_ident(c.name)} {tgt_type}{default_sql} {null_sql}")

    pk_sql = ""
    if pk.columns:
        pk_name = pk.name or f"PK_{tt}"
        cols = ", ".join([safe_ident(x) for x in pk.columns])
        pk_sql = f",\n  CONSTRAINT {safe_ident(pk_name)} PRIMARY KEY ({cols})"

    create = f"CREATE TABLE {full_name} (\n" + ",\n".join(col_lines) + pk_sql + "\n);\n"
    return create


def ddl_indexes(target_db_type: str, target_schema: str, target_table: str,
                indexes: List[IndexInfo]) -> str:
    ts = safe_ident(target_schema)
    tt = safe_ident(target_table)
    full_name = f"{ts}.{tt}" if ts else tt
    tgt_db = norm(target_db_type).lower()

    stmts = []
    for idx in indexes:
        idx_name = safe_ident(idx.name) if idx.name else f"IX_{tt}_{'_'.join(idx.columns[:2])}"
        cols = ", ".join([safe_ident(c) for c in idx.columns])
        unique = "UNIQUE " if idx.unique else ""
        if tgt_db == "oracle":
            stmts.append(f"CREATE {unique}INDEX {idx_name} ON {full_name} ({cols});\n")
        else:
            # SQL Server
            stmts.append(f"CREATE {unique}INDEX {idx_name} ON {full_name} ({cols});\n")
    return "".join(stmts)


def ddl_foreign_keys(target_db_type: str, target_schema: str, target_table: str,
                     fks: List[FKInfo]) -> str:
    ts = safe_ident(target_schema)
    tt = safe_ident(target_table)
    full_name = f"{ts}.{tt}" if ts else tt

    stmts = []
    for fk in fks:
        fk_name = safe_ident(fk.name) if fk.name else f"FK_{tt}"
        fk_cols = ", ".join([safe_ident(c) for c in fk.fk_columns])
        pk_full = f"{safe_ident(fk.pk_table_schema)}.{safe_ident(fk.pk_table)}" if fk.pk_table_schema else safe_ident(fk.pk_table)
        pk_cols = ", ".join([safe_ident(c) for c in fk.pk_columns])
        stmts.append(
            f"ALTER TABLE {full_name} ADD CONSTRAINT {fk_name} FOREIGN KEY ({fk_cols}) "
            f"REFERENCES {pk_full} ({pk_cols});\n"
        )
    return "".join(stmts)


def wrap_sql_for_target(target_db_type: str, sql: str) -> str:
    """
    Make the output a bit more "executable" for common CLIs.
    """
    tgt = norm(target_db_type).lower()
    if tgt.startswith("sql"):
        # SQLCMD likes GO; we will keep it optional and simple
        # If you want GO after each statement, you can split and add.
        return f"-- Generated for SQL Server\n\n{sql}"
    return f"-- Generated for Oracle\n\n{sql}"


# ----------------------------
# Main workflow
# ----------------------------
def generate_for_one_table(src_conn, src_db_type: str, tgt_db_type: str,
                           src_schema: str, src_table: str,
                           tgt_schema: str, tgt_table: str) -> str:
    cols, pk, idxs, fks = fetch_table_metadata(src_conn, src_db_type, src_schema, src_table)

    # Build DDL (table + indexes + FKs)
    ddl = ""
    ddl += ddl_create_table(
        target_db_type=tgt_db_type,
        target_schema=tgt_schema,
        target_table=tgt_table,
        columns=cols,
        pk=pk,
        src_db_type=src_db_type
    )
    ddl += "\n"
    ddl += ddl_indexes(tgt_db_type, tgt_schema, tgt_table, idxs)
    ddl += "\n"
    ddl += ddl_foreign_keys(tgt_db_type, tgt_schema, tgt_table, fks)
    ddl += "\n"
    return ddl


def parse_args():
    p = argparse.ArgumentParser(description="Schema Generator: SQLServer <-> Oracle using JDBC metadata")

    # Mode
    p.add_argument("--schema_mode", choices=["single", "multiple"], required=True)

    # Source
    p.add_argument("--source_db_type", required=True, help="sqlserver or oracle")
    p.add_argument("--source_host", required=True)
    p.add_argument("--source_port", required=True)
    p.add_argument("--source_database", required=True, help="SQLServer databaseName OR Oracle service name")
    p.add_argument("--source_username", required=True)
    p.add_argument("--source_password", required=True)
    p.add_argument("--source_schema", default="", help="Optional schema/owner")
    p.add_argument("--source_table_name", default="", help="Used in single mode")
    p.add_argument("--source_tables_file", default="", help="Used in multiple mode: file path in /tmp")

    # Target
    p.add_argument("--target_db_type", required=True, help="sqlserver or oracle")
    p.add_argument("--target_schema", default="", help="Optional schema/owner (where you will create)")
    p.add_argument("--output_file_name", required=True, help="Output .sql filename (written to /tmp)")

    # JDBC jars
    p.add_argument("--sqlserver_jar", required=True, help="Path to mssql-jdbc jar")
    p.add_argument("--oracle_jar", required=True, help="Path to ojdbc jar")

    # Output folder
    p.add_argument("--output_dir", default="/tmp", help="Default /tmp")
    return p.parse_args()


def main():
    args = parse_args()

    # Determine jar list for SOURCE connection only (we only read metadata from source)
    jars = []
    if norm(args.source_db_type).lower().startswith("sql"):
        jars = [args.sqlserver_jar]
    else:
        jars = [args.oracle_jar]

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / safe_ident(args.output_file_name)

    src_conn = None
    try:
        src_conn = jdbc_connect(
            db_type=args.source_db_type,
            host=args.source_host,
            port=args.source_port,
            database=args.source_database,
            username=args.source_username,
            password=args.source_password,
            jars=jars
        )

        ddl_all = ""

        if args.schema_mode == "single":
            if not norm(args.source_table_name):
                raise ValueError("single mode requires --source_table_name")
            src_schema = safe_ident(args.source_schema)
            src_table = safe_ident(args.source_table_name)

            # Target table name default = same as source
            tgt_schema = safe_ident(args.target_schema)
            tgt_table = src_table

            ddl_all += generate_for_one_table(
                src_conn=src_conn,
                src_db_type=args.source_db_type,
                tgt_db_type=args.target_db_type,
                src_schema=src_schema,
                src_table=src_table,
                tgt_schema=tgt_schema,
                tgt_table=tgt_table
            )

        else:
            # multiple
            if not norm(args.source_tables_file):
                raise ValueError("multiple mode requires --source_tables_file (file in /tmp)")
            tables = read_table_list_file(args.source_tables_file)

            for (sch, tab) in tables:
                src_schema = sch or safe_ident(args.source_schema)
                src_table = tab
                tgt_schema = safe_ident(args.target_schema)
                tgt_table = src_table

                ddl_all += f"\n-- =============================\n"
                ddl_all += f"-- TABLE: {src_schema+'.' if src_schema else ''}{src_table}\n"
                ddl_all += f"-- =============================\n"
                ddl_all += generate_for_one_table(
                    src_conn=src_conn,
                    src_db_type=args.source_db_type,
                    tgt_db_type=args.target_db_type,
                    src_schema=src_schema,
                    src_table=src_table,
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_table
                )

        final_sql = wrap_sql_for_target(args.target_db_type, ddl_all)

        with out_path.open("w", encoding="utf-8") as f:
            f.write(final_sql)

        print(f"SUCCESS: schema file generated at: {out_path}")

    finally:
        try:
            if src_conn is not None:
                src_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
