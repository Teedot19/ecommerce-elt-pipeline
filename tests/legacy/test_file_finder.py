from ingestion.utils.file_finder import list_csvs


def test_list_csvs(temp_dir):
    f1 = temp_dir / "a.csv"
    f2 = temp_dir / "b.csv"
    f3 = temp_dir / "ignore.txt"

    f1.write_text("x")
    f2.write_text("y")
    f3.write_text("z")

    result = list_csvs(temp_dir)
    assert len(result) == 2
    assert f1 in result
    assert f2 in result
    assert f3 not in result
