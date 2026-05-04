#!/usr/bin/env python3
"""
Copy a trimmed JaCoCo slice into core/src/test/fuzz/coverage_results/.

Expects a full report at /tmp/jacoco/report/ (as produced by
run_fuzz_with_coverage.sh). Keeps only kafka.server.KafkaApis and
RequestHandlerHelper pages plus jacoco-resources, rebuilds the
kafka.server package index tables for those two rows only, strips
broken Sessions links, and refreshes coverage_KafkaApis.xml and
coverage.csv (KafkaApis + RequestHandlerHelper rows only).
"""
from __future__ import annotations

import re
import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

TMP_HTML = Path("/tmp/jacoco/report/html")
TMP_XML = Path("/tmp/jacoco/report/coverage.xml")
TMP_CSV = Path("/tmp/jacoco/report/coverage.csv")
REPO_ROOT = Path(__file__).resolve().parents[4]
OUT = REPO_ROOT / "core/src/test/fuzz/coverage_results/html"


def read(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")


def extract_rows(body: str) -> list[str]:
    rows, pos = [], 0
    while True:
        m = re.search(r"<tr(?:\s[^>]*)?>", body[pos:])
        if not m:
            break
        start = pos + m.start()
        end = body.find("</tr>", start)
        if end < 0:
            break
        end += len("</tr>")
        rows.append(body[start:end])
        pos = end
    return rows


def row_by_href(body: str, href: str) -> str:
    for tr in extract_rows(body):
        if href in tr:
            return tr
    raise FileNotFoundError(f"no table row containing {href!r} under {TMP_HTML}")


def parse_bar_pair(td_html: str) -> tuple[int, int]:
    reds = re.findall(r"redbar\.gif[^>]*title=\"([^\"]+)\"", td_html)
    greens = re.findall(r"greenbar\.gif[^>]*title=\"([^\"]+)\"", td_html)

    def n(s: str) -> int:
        return int(s.replace(",", ""))

    rm = n(reds[0]) if reds else 0
    gc = n(greens[0]) if greens else 0
    return rm, gc


def parse_row(tr: str) -> dict:
    bars = re.findall(r"<td class=\"bar\"[^>]*>(.*?)</td>", tr, re.DOTALL)
    if len(bars) < 2:
        raise ValueError("expected two bar cells in row")
    inst_m, inst_c = parse_bar_pair(bars[0])
    br_m, br_c = parse_bar_pair(bars[1])
    pairs = []
    for m in re.finditer(
        r"<td class=\"ctr1\"[^>]*>([^<]*)</td>\s*<td class=\"ctr2\"[^>]*>([^<]*)</td>",
        tr,
    ):
        a, b = m.group(1).strip().replace(",", ""), m.group(2).strip().replace(",", "")
        pairs.append((int(a), int(b)))
    if len(pairs) != 4:
        raise ValueError(f"expected 4 ctr1/ctr2 pairs, got {len(pairs)}")
    return {"inst": (inst_m, inst_c), "br": (br_m, br_c), "pairs": pairs}


def cov_pct(covered: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{round(100.0 * covered / total)}%"


def bar_width(missed: int, covered: int, maxw: int = 120) -> tuple[int, int]:
    tot = missed + covered
    if tot == 0:
        return 0, 0
    rw = max(1, round(maxw * missed / tot))
    gw = max(1, maxw - rw)
    return rw, gw


def fmt_num(n: int) -> str:
    return format(n, ",")


def bar_cell(missed: int, covered: int, rw: int, gw: int) -> str:
    parts = []
    if missed:
        parts.append(
            f'<img src="../jacoco-resources/redbar.gif" width="{rw}" height="10" '
            f'title="{fmt_num(missed)}" alt="{fmt_num(missed)}"/>'
        )
    if covered:
        parts.append(
            f'<img src="../jacoco-resources/greenbar.gif" width="{gw}" height="10" '
            f'title="{fmt_num(covered)}" alt="{fmt_num(covered)}"/>'
        )
    return "".join(parts)


def build_tfoot(im: int, ic: int, bm: int, bc: int, pairs: list[tuple[int, int]]) -> str:
    rw, gw = bar_width(im, ic)
    rb, gb = bar_width(bm, bc)
    cov_i = cov_pct(ic, im + ic)
    cov_b = cov_pct(bc, bm + bc) if (bm + bc) else "n/a"
    (cx_m, cx_t), (ln_m, ln_t), (mt_m, mt_t), (cl_m, cl_t) = pairs
    return (
        "<tfoot><tr><td>Total</td>"
        f'<td class="bar">{bar_cell(im, ic, rw, gw)}</td>'
        f'<td class="ctr2">{cov_i}</td>'
        f'<td class="bar">{bar_cell(bm, bc, rb, gb)}</td>'
        f'<td class="ctr2">{cov_b}</td>'
        f'<td class="ctr1">{fmt_num(cx_m)}</td><td class="ctr2">{fmt_num(cx_t)}</td>'
        f'<td class="ctr1">{fmt_num(ln_m)}</td><td class="ctr2">{fmt_num(ln_t)}</td>'
        f'<td class="ctr1">{fmt_num(mt_m)}</td><td class="ctr2">{fmt_num(mt_t)}</td>'
        f'<td class="ctr1">{fmt_num(cl_m)}</td><td class="ctr2">{fmt_num(cl_t)}</td>'
        "</tr></tfoot>"
    )


def tfoot_totals(rows: list[dict]) -> tuple[int, int, int, int, list[tuple[int, int]]]:
    im = sum(r["inst"][0] for r in rows)
    ic = sum(r["inst"][1] for r in rows)
    bm = sum(r["br"][0] for r in rows)
    bc = sum(r["br"][1] for r in rows)
    merged = []
    for i in range(4):
        merged.append((sum(r["pairs"][i][0] for r in rows), sum(r["pairs"][i][1] for r in rows)))
    return im, ic, bm, bc, merged


def build_index_page(
    body_inner: str,
    link_href: str,
    link_class: str,
    link_text: str,
) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head>
  <meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
  <link rel="stylesheet" href="jacoco-resources/report.css" type="text/css"/>
  <link rel="shortcut icon" href="jacoco-resources/report.gif" type="image/gif"/>
  <title>kafka.server</title>
  <script type="text/javascript" src="jacoco-resources/sort.js"></script>
</head>
<body onload="initialSort(['breadcrumb', 'coveragetable'])">
<div class="breadcrumb" id="breadcrumb">
  <span class="info"><a href="{link_href}" class="{link_class}">{link_text}</a></span>
  <span class="el_report">KafkaApis fuzz coverage</span> &gt; <span class="el_package">kafka.server</span>
</div>
<h1>kafka.server</h1>
{body_inner}
<div class="footer"><span class="right">Created with <a href="http://www.jacoco.org/jacoco">JaCoCo</a> 0.8.12</span></div>
</body>
</html>
"""


THEAD = """<table class="coverage" cellspacing="0" id="coveragetable">
<thead><tr>
<td class="sortable" id="a" onclick="toggleSort(this)">Element</td>
<td class="down sortable bar" id="b" onclick="toggleSort(this)">Missed Instructions</td>
<td class="sortable ctr2" id="c" onclick="toggleSort(this)">Cov.</td>
<td class="sortable bar" id="d" onclick="toggleSort(this)">Missed Branches</td>
<td class="sortable ctr2" id="e" onclick="toggleSort(this)">Cov.</td>
<td class="sortable ctr1" id="f" onclick="toggleSort(this)">Missed</td>
<td class="sortable ctr2" id="g" onclick="toggleSort(this)">Cxty</td>
<td class="sortable ctr1" id="h" onclick="toggleSort(this)">Missed</td>
<td class="sortable ctr2" id="i" onclick="toggleSort(this)">Lines</td>
<td class="sortable ctr1" id="j" onclick="toggleSort(this)">Missed</td>
<td class="sortable ctr2" id="k" onclick="toggleSort(this)">Methods</td>
<td class="sortable ctr1" id="l" onclick="toggleSort(this)">Missed</td>
<td class="sortable ctr2" id="m" onclick="toggleSort(this)">Classes</td>
</tr></thead>
<tbody>
"""


def main() -> int:
    if not TMP_HTML.is_dir():
        print(f"[refresh] skip: {TMP_HTML} not found", file=sys.stderr)
        return 0
    full_src = read(TMP_HTML / "kafka.server/index.source.html")
    full_cls = read(TMP_HTML / "kafka.server/index.html")
    body_src = re.search(r"<tbody>(.*?)</tbody>", full_src, re.DOTALL)
    body_cls = re.search(r"<tbody>(.*?)</tbody>", full_cls, re.DOTALL)
    if not body_src or not body_cls:
        print("[refresh] could not parse package index tbody", file=sys.stderr)
        return 1
    body_src = body_src.group(1)
    body_cls = body_cls.group(1)

    tr_ka_s = row_by_href(body_src, "KafkaApis.scala.html")
    tr_rh_s = row_by_href(body_src, "RequestHandlerHelper.scala.html")
    tr_ka_c = row_by_href(body_cls, "KafkaApis.html")
    tr_rh_c = row_by_href(body_cls, "RequestHandlerHelper.html")

    rows_src = [parse_row(tr_ka_s), parse_row(tr_rh_s)]
    rows_cls = [parse_row(tr_ka_c), parse_row(tr_rh_c)]

    im, ic, bm, bc, pairs_src = tfoot_totals(rows_src)
    tfoot_src = build_tfoot(im, ic, bm, bc, pairs_src)
    im2, ic2, bm2, bc2, pairs_cls = tfoot_totals(rows_cls)
    tfoot_cls = build_tfoot(im2, ic2, bm2, bc2, pairs_cls)

    idx_src = build_index_page(
        THEAD + tr_ka_s + tr_rh_s + tfoot_src + "</table>",
        "index.html",
        "el_class",
        "Classes",
    )
    idx_cls = build_index_page(
        THEAD + tr_ka_c + tr_rh_c + tfoot_cls + "</table>",
        "index.source.html",
        "el_source",
        "Source Files",
    )

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "kafka.server/index.source.html").write_text(idx_src, encoding="utf-8")
    (OUT / "kafka.server/index.html").write_text(idx_cls, encoding="utf-8")

    for name in (
        "KafkaApis.scala.html",
        "RequestHandlerHelper.scala.html",
        "KafkaApis.html",
        "RequestHandlerHelper.html",
    ):
        shutil.copy2(TMP_HTML / "kafka.server" / name, OUT / "kafka.server" / name)
        p = OUT / "kafka.server" / name
        txt = read(p)
        txt = re.sub(
            r'<span class="info"><a href="../jacoco-sessions.html" class="el_session">Sessions</a></span>',
            '<span class="info"></span>',
            txt,
        )
        p.write_text(txt, encoding="utf-8")

    jac_dst = OUT / "jacoco-resources"
    if jac_dst.exists():
        shutil.rmtree(jac_dst)
    shutil.copytree(TMP_HTML / "jacoco-resources", jac_dst)

    if TMP_XML.is_file():
        tree = ET.parse(TMP_XML)
        root = tree.getroot()
        new_root = ET.Element("report", root.attrib)
        for pkg in root.findall("package"):
            if pkg.get("name") == "kafka/server":
                new_root.append(pkg)
                break
        out_xml = REPO_ROOT / "core/src/test/fuzz/coverage_results/coverage_KafkaApis.xml"
        ET.ElementTree(new_root).write(out_xml, encoding="utf-8", xml_declaration=True)

    if TMP_CSV.is_file():
        lines = TMP_CSV.read_text(encoding="utf-8").splitlines()
        hdr = lines[0]
        out_lines = [hdr]
        for line in lines[1:]:
            if ",kafka/server/KafkaApis," in line or ",kafka/server/RequestHandlerHelper," in line:
                out_lines.append(line)
        (REPO_ROOT / "core/src/test/fuzz/coverage_results/coverage.csv").write_text(
            "\n".join(out_lines) + "\n", encoding="utf-8"
        )

    print(f"[refresh] updated {OUT.relative_to(REPO_ROOT)} and coverage_results/*.{{xml,csv}}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
