from strand.repo import Repo


def test_init_and_commit_local(tmp_path):
    root = str(tmp_path)
    repo = Repo.init(root)
    assert repo.head_commit() is None

    commit_id = repo.commit(message="first")
    assert isinstance(commit_id, str) and len(commit_id) == 64
    assert repo.head_commit() == commit_id

    commits = repo.log(limit=10)
    assert len(commits) == 1
    assert commits[0].message == "first"
