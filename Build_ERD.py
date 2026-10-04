"""Render the star schema in Star_Schema_ETL.sql as an ER diagram.

eralchemy reads a live database or its own .er markup -- it cannot read a
.sql file, and pointing it at BigQuery would show no relationships, because
BigQuery accepts PRIMARY KEY / FOREIGN KEY only as NOT ENFORCED metadata and
does not expose it the way SQLAlchemy's reflection expects. So this parses
Section 1 of Star_Schema_ETL.sql -- the CREATE TABLE bodies, the ADD COLUMN
migrations that follow them, and those NOT ENFORCED key clauses -- into
SQLAlchemy metadata, and hands that to eralchemy.

The DDL stays the single source of truth: add a column or a key in
Star_Schema_ETL.sql, rerun this, and the diagram follows.

    python Build_ERD.py                  # docs/ERD_Star_Schema.{svg,er,md}
    python Build_ERD.py --scope all      # also staging, etl_load_log, etl_lease
    python Build_ERD.py -o erd.png       # any format eralchemy supports

Needs eralchemy, the graphviz Python package and a Graphviz install (see
requirements-dev.txt). If `dot` is not on PATH, set GRAPHVIZ_BIN to the
directory holding it; common Anaconda and Program Files locations are tried
automatically.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from pathlib import Path

import sqlalchemy as sa

HERE = Path(__file__).resolve().parent
SQL_FILE = HERE / 'Star_Schema_ETL.sql'
DATASET = 'crashes'

# Tables that are not part of the dimensional model: the landing table the
# extractor truncates into, and the two ETL control tables.
NON_STAR = {'stg_crash_person', 'etl_load_log', 'etl_lease'}


# ------------------------------------------------------------------
# Graphviz
# ------------------------------------------------------------------

def ensure_graphviz() -> None:
    """Put `dot` on PATH if it is installed somewhere PATH does not mention.

    Named dot.exe, not dot: on Windows `shutil.which('dot')` happily returns
    Anaconda's dot.BAT shim, which CreateProcess cannot launch, and graphviz
    then fails with a bare WinError 2.
    """
    exe = 'dot.exe' if os.name == 'nt' else 'dot'
    if shutil.which(exe):
        return
    candidates = []
    if os.environ.get('GRAPHVIZ_BIN'):
        candidates.append(Path(os.environ['GRAPHVIZ_BIN']))
    candidates += [
        Path(sys.prefix) / 'Library' / 'bin',          # conda env
        Path.home() / 'anaconda3' / 'Library' / 'bin',
        Path.home() / 'miniconda3' / 'Library' / 'bin',
        Path(r'C:\Program Files\Graphviz\bin'),
        Path('/usr/local/bin'),
    ]
    for d in candidates:
        if (d / exe).is_file():
            os.environ['PATH'] = str(d) + os.pathsep + os.environ['PATH']
            return
    raise SystemExit(
        f'Graphviz not found: no {exe} on PATH or in the usual places.\n'
        'Install Graphviz (conda install graphviz, or graphviz.org/download)\n'
        'and set GRAPHVIZ_BIN to the directory holding it.')


# ------------------------------------------------------------------
# DDL parsing
# ------------------------------------------------------------------

LINE_COMMENT = re.compile(r'--[^\n]*')
CREATE_RE = re.compile(
    r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?' + DATASET + r'\.(\w+)\s*\(',
    re.IGNORECASE)
CTAS_RE = re.compile(
    r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?' + DATASET
    + r'\.(\w+)\s+AS\s+(SELECT.*?);',
    re.IGNORECASE | re.DOTALL)
ALTER_RE = re.compile(
    r'ALTER\s+TABLE\s+' + DATASET + r'\.(\w+)\s+(.*?);',
    re.IGNORECASE | re.DOTALL)
ADD_COLUMN_RE = re.compile(
    r'ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+(\w+)',
    re.IGNORECASE)
COLUMN_RE = re.compile(
    r'^(\w+)\s+(\w+(?:\s*<[^>]*>)?(?:\([^)]*\))?)\s*(NOT\s+NULL)?$',
    re.IGNORECASE)
PK_RE = re.compile(r'^PRIMARY\s+KEY\s*\(([^)]*)\)', re.IGNORECASE)
FK_RE = re.compile(
    r'^FOREIGN\s+KEY\s*\(([^)]*)\)\s*REFERENCES\s+(?:' + DATASET
    + r'\.)?(\w+)\s*\(([^)]*)\)',
    re.IGNORECASE)
CAST_NULL_RE = re.compile(r'CAST\s*\(\s*NULL\s+AS\s+(\w+)\s*\)', re.IGNORECASE)
ALIAS_RE = re.compile(r'\sAS\s+(\w+)\s*$', re.IGNORECASE)


class Parsed:
    """One table as the DDL describes it."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.columns: list[tuple[str, str, bool]] = []   # name, type, not_null
        self.primary_key: list[str] = []
        self.foreign_keys: list[tuple[str, str, str]] = []  # col, table, col

    def add(self, name: str, type_: str, not_null: bool = False) -> None:
        if any(c[0] == name for c in self.columns):
            return                                       # migration no-op
        self.columns.append((name, type_, not_null))


def split_top_level(body: str) -> list[str]:
    """Split on the commas that are not inside parentheses."""
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(body):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif ch == ',' and depth == 0:
            parts.append(body[start:i])
            start = i + 1
    parts.append(body[start:])
    return [' '.join(p.split()) for p in parts if p.strip()]


def balanced_body(sql: str, open_paren: int) -> tuple[str, int]:
    """Return the text inside the parens starting at `open_paren`, and its end."""
    depth = 0
    for i in range(open_paren, len(sql)):
        if sql[i] == '(':
            depth += 1
        elif sql[i] == ')':
            depth -= 1
            if depth == 0:
                return sql[open_paren + 1:i], i
    raise ValueError(f'unbalanced parentheses at offset {open_paren}')


def parse_ddl(sql: str) -> dict[str, Parsed]:
    sql = LINE_COMMENT.sub('', sql)
    tables: dict[str, Parsed] = {}

    for m in CREATE_RE.finditer(sql):
        table = Parsed(m.group(1))
        body, _ = balanced_body(sql, m.end() - 1)
        for item in split_top_level(body):
            pk = PK_RE.match(item)
            if pk:
                table.primary_key += [c.strip() for c in pk.group(1).split(',')]
                continue
            fk = FK_RE.match(item)
            if fk:
                table.foreign_keys.append((fk.group(1).strip(), fk.group(2),
                                           fk.group(3).strip()))
                continue
            col = COLUMN_RE.match(item)
            if col:
                table.add(col.group(1), col.group(2).upper(), bool(col.group(3)))
            else:
                print(f'  ! skipped in {table.name}: {item}', file=sys.stderr)
        tables[table.name] = table

    # CREATE TABLE ... AS SELECT: etl_lease is created and seeded atomically,
    # so its columns are the select list, typed by their CAST(NULL AS t).
    for m in CTAS_RE.finditer(sql):
        if m.group(1) in tables:
            continue
        table = Parsed(m.group(1))
        select = m.group(2)[len('SELECT'):]
        for item in split_top_level(select):
            alias = ALIAS_RE.search(item)
            if not alias:
                continue
            cast = CAST_NULL_RE.search(item)
            table.add(alias.group(1),
                      cast.group(1).upper() if cast else 'STRING')
        tables[table.name] = table

    # The --setup migrations: columns added to tables that predate them.
    for m in ALTER_RE.finditer(sql):
        table = tables.get(m.group(1))
        if table is None:
            continue
        for col, type_ in ADD_COLUMN_RE.findall(m.group(2)):
            table.add(col, type_.upper())

    return tables


# ------------------------------------------------------------------
# SQLAlchemy metadata
# ------------------------------------------------------------------

class BQType(sa.types.UserDefinedType):
    """Carries the BigQuery type name through to the diagram verbatim.

    eralchemy labels a column with str(column.type), so the alternative is
    reading SQLAlchemy's VARCHAR on a BigQuery STRING.
    """

    cache_ok = True

    def __init__(self, name: str) -> None:
        self.name = name

    def get_col_spec(self, **kw) -> str:
        return self.name

    def __str__(self) -> str:
        return self.name


def build_metadata(tables: dict[str, Parsed], keep: set[str]) -> sa.MetaData:
    metadata = sa.MetaData()
    for name in tables:
        if name not in keep:
            continue
        parsed = tables[name]
        targets = {fk[0]: fk for fk in parsed.foreign_keys
                   if fk[1] in keep}          # a FK to an excluded table is dropped
        columns = []
        for col, type_, not_null in parsed.columns:
            args = [col, BQType(type_)]
            if col in targets:
                args.append(sa.ForeignKey(f'{targets[col][1]}.{targets[col][2]}'))
            columns.append(sa.Column(
                *args,
                primary_key=col in parsed.primary_key,
                nullable=not not_null,
            ))
        sa.Table(name, metadata, *columns)
    return metadata


# ------------------------------------------------------------------
# Rendering
# ------------------------------------------------------------------

def write_mermaid(metadata: sa.MetaData, path: Path, title: str) -> None:
    """A GitHub-native ```mermaid block, which renders without an image.

    eralchemy's own .md output is the markup commented out plus an
    <img> pointing at mermaid.ink; unwrap it and fence it instead.
    """
    from eralchemy import render_er

    blob = render_er(metadata, None, mode='mermaid_er', sort_mode='original')
    if blob is None:
        raise SystemExit('eralchemy produced no mermaid output')
    markup = blob.decode('utf-8').split('<!--', 1)[1].split('-->', 1)[0].strip()
    path.write_text(
        f'# {title}\n\n'
        f'Generated by `Build_ERD.py` from `Star_Schema_ETL.sql`. Do not edit.\n\n'
        f'```mermaid\n{markup}\n```\n',
        encoding='utf-8', newline='\n')


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--scope', choices=['star', 'all'], default='star',
                    help="'star' is the fact and its dimensions; 'all' adds "
                         'staging, etl_load_log and etl_lease')
    ap.add_argument('-o', '--out', action='append', metavar='FILE',
                    help='output file; repeatable. Format comes from the '
                         'extension (.svg .png .pdf .er .dot .md .puml)')
    ap.add_argument('--title', help='title drawn on the diagram')
    ap.add_argument('--layout', choices=['tb', 'lr'], default='tb',
                    help="'tb' puts the dimensions in a row above the fact "
                         "(the usual star picture); 'lr' is eralchemy's "
                         'default, a tall left-hand column')
    ap.add_argument('--crowfoot', action='store_true',
                    help='crow\'s-foot ends instead of "1" / "0..N" labels')
    args = ap.parse_args()

    tables = parse_ddl(SQL_FILE.read_text(encoding='utf-8'))
    keep = set(tables) if args.scope == 'all' else set(tables) - NON_STAR
    metadata = build_metadata(tables, keep)

    stem = 'ERD_Star_Schema' if args.scope == 'star' else 'ERD_All_Tables'
    title = args.title or (
        'Cincinnati crash star schema (BigQuery dataset: crashes)'
        if args.scope == 'star' else
        'Cincinnati crash ETL: every table in Star_Schema_ETL.sql')
    outputs = [Path(p) for p in (args.out or
                                 [f'docs/{stem}.svg', f'docs/{stem}.er',
                                  f'docs/{stem}.md'])]

    # .er, .dot, .md and .puml are text; everything else is drawn by Graphviz.
    if any(o.suffix not in {'.er', '.dot', '.md', '.puml'} for o in outputs):
        ensure_graphviz()
    from eralchemy import render_er
    from eralchemy.cst import config, dot_crowfoot, dot_star_primary, dot_top_down

    if args.layout == 'tb':
        dot_top_down()
    if args.crowfoot:
        dot_crowfoot()
    # '*' for a key, not eralchemy's default <u>: every row already carries a
    # cell border, so an underlined name is indistinguishable from the rest.
    dot_star_primary()
    # Room for the title, and enough rank separation that seven fanned-in
    # edges stay apart. Patched into eralchemy's graph header rather than
    # replacing it, so the rest of its styling survives a version bump.
    config['DOT_GRAPH_BEGINNING'] = re.sub(
        r'graph \[rankdir=(TB|LR)\]',
        r'graph [rankdir=\1, ranksep=1.2, nodesep=0.5, pad=0.4, fontsize=20]',
        config['DOT_GRAPH_BEGINNING'])

    n_cols = sum(len(t.columns) for t in metadata.tables.values())
    n_rels = sum(len(t.foreign_keys) for t in metadata.tables.values())
    print(f'{len(metadata.tables)} tables, {n_cols} columns, '
          f'{n_rels} relationships')

    for out in outputs:
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.suffix == '.md':
            write_mermaid(metadata, out, title)
        else:
            # render_er reports its own failures to stderr and returns None,
            # so check the file rather than trusting the call.
            before = out.stat().st_mtime if out.exists() else None
            # Graphviz sizes the title band to the text alone, so the tallest
            # table's top edge ends up against it. A trailing blank line is
            # the only spacing knob dot offers here.
            drawn = title if out.suffix == '.er' else title + r'\n '
            render_er(metadata, str(out), title=drawn, sort_mode='original')
            if not out.exists() or out.stat().st_mtime == before:
                raise SystemExit(f'failed to write {out}')
        print(f'  wrote {out}')


if __name__ == '__main__':
    main()
