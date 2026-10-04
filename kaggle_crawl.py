import csv
import shutil
import subprocess
import sys
import time
from pathlib import Path

STARTED = time.monotonic()

#đổi shard_id từ 1-30
SHARD_ID = 1

TIME_BUDGET_HOURS = 12

INPUT = Path("/kaggle/input")
WORK = Path("/kaggle/working")
PROJECT = WORK / "project"

# Chọn shard đã được tạo sẵn.
shard = next(INPUT.rglob(f"shard_{SHARD_ID:05d}.csv"))

# Tìm code trong Dataset project.
source_script = next(INPUT.rglob("src/download_data.py"))
source_project = source_script.parent.parent

# Chỉ copy code và shard, không copy toàn bộ project.
shutil.copytree(
    source_project / "src",
    PROJECT / "src",
    dirs_exist_ok=True,
)

metadata = PROJECT / "data/metadata"
metadata.mkdir(parents=True, exist_ok=True)
shutil.copyfile(shard, metadata / "archive_manifest.csv")

subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q", "boto3"],
    check=True,
)

remaining = max(
    1,
    TIME_BUDGET_HOURS * 3600 - (time.monotonic() - STARTED),
)

try:
    result = subprocess.run(
        [sys.executable, "-u", str(PROJECT / "src/download_data.py")],
        cwd=PROJECT,
        timeout=remaining,
    )
    print("Mã kết thúc downloader:", result.returncode)
except subprocess.TimeoutExpired:
    print("Đã dừng tải theo thời gian dành cho phiên.")

# Kiểm tra lại toàn bộ shard để ghi đủ:
# - File tải lỗi
# - File đang dở
# - File chưa kịp tải khi hết giờ
fail_csv = WORK / f"downloaded_fail_shard_{SHARD_ID:05d}.csv"
raw = PROJECT / "data/raw"
finished = failed = 0

with shard.open(newline="", encoding="utf-8-sig") as source, \
     fail_csv.open("w", newline="", encoding="utf-8") as output:

    writer = csv.writer(output)
    writer.writerow(["key", "size"])

    for row in csv.DictReader(source):
        key, size = row["key"], int(row["size"])
        relative = key.removeprefix("records/csv.gz/")
        path = raw / relative

        if path.is_file() and path.stat().st_size == size:
            finished += 1
        else:
            writer.writerow([key, size])
            failed += 1

print(f"Shard: {SHARD_ID}")
print(f"Hoàn thành: {finished:,}")
print(f"Lỗi hoặc chưa hoàn thành: {failed:,}")
print(f"Danh sách tải lại: {fail_csv}")
print(f"Dữ liệu: {raw}")

import csv
import time
import zipfile

zip_started = time.perf_counter()

zip_path = WORK / f"shard_{SHARD_ID:05d}.zip"
temporary_zip = WORK / f"shard_{SHARD_ID:05d}.zip.part"

raw_root = raw.resolve()
allowed_root = (WORK / "project/data/raw").resolve()

if raw_root != allowed_root or not raw_root.is_relative_to(WORK.resolve()):
    raise ValueError("Thư mục raw không nằm đúng trong project output.")

packed_paths = []

# Ghi ZIP tạm trước; file .csv.gz đã nén nên chỉ cần ZIP_STORED.
with zipfile.ZipFile(
    temporary_zip,
    mode="w",
    compression=zipfile.ZIP_STORED,
    allowZip64=True,
) as archive:

    with shard.open(newline="", encoding="utf-8-sig") as source:
        for row in csv.DictReader(source):
            relative = row["key"].removeprefix("records/csv.gz/")
            path = (raw_root / relative).resolve()

            if not path.is_relative_to(raw_root):
                raise ValueError(f"Đường dẫn nằm ngoài raw: {row['key']}")

            if path.is_file() and path.stat().st_size == int(row["size"]):
                archive.write(
                    path,
                    arcname="data/raw/" + path.relative_to(raw_root).as_posix(),
                )
                packed_paths.append(path)

    archive.write(shard, arcname=shard.name)
    archive.write(fail_csv, arcname=fail_csv.name)

# Kiểm tra ZIP trước khi xóa bản thô.
with zipfile.ZipFile(temporary_zip, "r") as archive:
    bad_file = archive.testzip()
    if bad_file is not None:
        raise OSError(f"ZIP bị lỗi ở file: {bad_file}")

temporary_zip.replace(zip_path)

# Chỉ xóa đúng những file đã được đóng gói thành công.
for path in packed_paths:
    path.unlink()

print(f"ZIP: {zip_path}")
print(f"Đã đóng gói: {len(packed_paths):,} file dữ liệu")
print(f"Dung lượng: {zip_path.stat().st_size / 1e6:.2f} MB")
print(f"Thời gian đóng gói + kiểm tra: {time.perf_counter() - zip_started:.2f} giây")
print(f"CSV fail vẫn được giữ riêng tại: {fail_csv}")