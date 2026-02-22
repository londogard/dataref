from strand.repo import Repo


def test_dataset_ref_clone_is_zero_copy_local(tmp_path):
    repo = Repo.init(str(tmp_path))
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "a.txt").write_text("hello")

    repo.snapshot_dataset(dataset="demo", dataset_root=str(dataset), ref="main")
    before = set((tmp_path / ".strand" / "objects").iterdir())

    repo.clone_dataset_ref(dataset="demo", source_ref="main", target_ref="staging")
    after = set((tmp_path / ".strand" / "objects").iterdir())

    assert repo.dataset_head("demo", "main") == repo.dataset_head("demo", "staging")
    assert before == after


def test_list_dataset_files_reads_snapshot_manifest(tmp_path):
    repo = Repo.init(str(tmp_path))
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "b.txt").write_text("world")
    (dataset / "a.txt").write_text("hello")

    repo.snapshot_dataset(dataset="demo", dataset_root=str(dataset), ref="main")
    files = repo.list_dataset_files(dataset="demo", ref="main")

    assert files == ["a.txt", "b.txt"]
