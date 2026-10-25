"""Summarize completed structure checks CSVs without Spark or third-party packages."""

import csv
import heapq
import json
from pathlib import Path


def csv_rows(report, name):
    for path in sorted((report / name).glob("part-*.csv")):
        with path.open(encoding="utf-8", newline="") as stream:
            yield from csv.DictReader(stream)


def table(rows, columns):
    def escape(value):
        return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    lines.extend("| " + " | ".join(escape(row.get(c, "")) for c in columns) + " |" for row in rows)
    return "\n".join(lines)


def summarize(report):
    report = Path(report).absolute()
    metadata = json.loads((report / "run.json").read_text())
    if metadata["status"] != "complete":
        raise ValueError(f"Run is not complete: {metadata['status']}")
    summary = list(csv_rows(report, "quality_summary"))
    metrics = {row["issue"]: int(row["rows"]) for row in summary}
    overlaps = list(csv_rows(report, "issue_overlap"))
    assert sum(int(row["count"]) for row in overlaps) == metrics["rows_with_any_issue"], "Overlap total mismatch"
    # Every row with an issue occurs once per matching issue in these reports.
    by_issue = {}
    for row in csv_rows(report, "issues_by_sensor"):
        by_issue[row["issue"]] = by_issue.get(row["issue"], 0) + int(row["count"])
    for name, count in metrics.items():
        if name not in ("all_rows", "rows_with_any_issue"):
            assert by_issue.get(name, 0) == count, f"Sensor total mismatch: {name}"
    coordinate = heapq.nlargest(10, csv_rows(report, "coordinate_errors_by_sensor"),
                              key=lambda row: int(row["coordinate_issue_rows"]))
    patterns = sorted(csv_rows(report, "parse_error_patterns"), key=lambda row: -int(row["count"]))
    shifts = heapq.nlargest(20, csv_rows(report, "suspected_column_shift"), key=lambda row: int(row["count"]))
    total = metrics["all_rows"]
    lines = [
        "# structure checks — Kết quả chẩn đoán chất lượng", "",
        f"Run: `{report.name}`; hoàn tất UTC: {metadata.get('finished_utc')}; layout: {metadata.get('layout', 'flat')}; files: {metadata['files']:,}.", "",
        f"Phạm vi: **{total:,} dòng**; limit_files={metadata['limit_files']}; months={metadata.get('months')}. "
        "Các cờ có thể giao nhau; fraction là tỷ lệ [0, 1].", "",
        "## Tổng hợp", "", table(summary, ["issue", "rows", "fraction_of_all_rows"]), "",
        "## Kiểm tra tính nhất quán", "",
        "- Tổng các nhóm overlap khớp số dòng có ít nhất một cờ.",
        "- Tổng theo sensor khớp từng cờ trong quality_summary.", "",
        "## Pattern missing / parse lỗi", "", table(patterns, ["issue", "pattern", "count"]), "",
        "Xem `parse_error_values/` để biết top giá trị chuỗi gốc; `diagnostic_samples/` chứa raw/typed và `source_block`; `datetime_offset_repair/` kiểm tra offset `±HH:-MM` theo mùa.", "",
        "## Top sensor có lỗi tọa độ", "",
        table(coordinate, ["location_id", "sensors_id", "coordinate_issue_rows", "latitude_out_of_range", "longitude_out_of_range", "swap_candidate"]), "",
        "Xem `coordinate_errors_by_location/` theo location; bối cảnh tọa độ hợp lệ của mọi location nằm trong `dim_location` (build_location_reference.py).", "",
        "## Nhóm nghi lệch cột", "", table(shifts, ["parameter", "units", "datetime_cast_failed", "value_cast_failed", "lat_cast_failed", "lon_cast_failed", "count"]), "",
        "## Diễn giải và giới hạn", "",
        "- Báo cáo xác nhận số lượng và nhóm xuất hiện các cờ; chưa xác nhận nguyên nhân ở CSV gốc.",
        "- swap_candidate chỉ cho biết cặp sau đảo nằm trong miền, không chứng minh tọa độ đúng.",
        "- suspected_column_shift dùng heuristic parameter dạng số hoặc units chứa token parameter; cần xem mẫu và đối chiếu nguồn.",
        "- Không có mapping CSV Bronze/staging trong Near-Silver hiện tại; source_block chỉ trỏ tới block DataNode (xem manifest block recovery).",
        "- Không sửa hoặc loại bỏ dữ liệu trong structure checks. Việc xác nhận nguyên nhân từ Bronze còn chờ nguồn dữ liệu.", "",
    ]
    (report / "SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")
    return metadata


def update_tracking(tracking, report, metadata):
    tracking, report = Path(tracking), Path(report).absolute()
    text = tracking.read_text(encoding="utf-8")
    marker = f"<!-- structure-result:{report.name} -->"
    if marker in text:
        return
    import os
    relative = Path(os.path.relpath(report / "SUMMARY.md", tracking.absolute().parent)).as_posix()
    status = metadata["status"]
    if status == "complete":
        state = "Đã có báo cáo full và kiểm tra tổng; chờ đối chiếu Bronze"
        result = f"Báo cáo: [SUMMARY.md]({relative}). Số dòng: {metadata['overview']['rows']:,}. "
        result += "Chưa xác nhận nguyên nhân từ CSV gốc; chưa chốt cleaning."
    else:
        state = f"Run {status}; chưa hoàn tất"
        result = f"Chi tiết trong `{report.name}/run.json`: {metadata.get('error', 'Không có kết quả hoàn tất')}."
    text = text.replace("Đã triển khai, pilot pass; đang chạy full", state)
    text = text.replace("**Đã triển khai và kiểm tra; đang chạy full**", f"**{state}**")
    text = text.replace(
        f"| `{report.name}` | Toàn bộ 16.711 file | Đang chạy với logic tối ưu; fixture Spark đã pass lại |",
        f"| `{report.name}` | Toàn bộ 16.711 file | {state} |",
    )
    note = f"{marker}\n\n### Kết quả tự động — {report.name}\n\n{result}\n\n"
    text = text.replace("## duplicate checks —", note + "## duplicate checks —", 1)
    if status == "complete":
        text = text.replace("Kết quả / quyết định: Chờ báo cáo full.", "Kết quả / quyết định: Đã có báo cáo full, xem phần kết quả tự động bên dưới.")
    tracking.write_text(text, encoding="utf-8")
