from pathlib import Path
def test_sql_files_exist_and_nonempty():
    files = list(Path("member4_analytics/sql").glob("*.sql"))
    assert len(files) >= 6
    assert all(f.read_text().strip() for f in files)