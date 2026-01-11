from strand.repo import Repo


def test_snapshot_and_diff_local(tmp_path):
    root = str(tmp_path)
    repo = Repo.init(root)

    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "a.txt").write_text("hello")
    (dataset / "b.txt").write_text("world")

    c1 = repo.snapshot(dataset_root=str(dataset), message="snap1")

    # modify, remove, add
    (dataset / "a.txt").write_text("hello!!!")
    (dataset / "b.txt").unlink()
    (dataset / "c.txt").write_text("new")

    c2 = repo.snapshot(dataset_root=str(dataset), message="snap2")

    d = repo.diff_snapshots(c1, c2)
    assert "c.txt" in d["added"]
    assert "b.txt" in d["removed"]
    assert "a.txt" in d["modified"]
