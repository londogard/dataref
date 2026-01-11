from strand.hash import canonical_dumps, sha256_json


def test_canonical_dumps_is_stable_key_order():
    a = {"b": 1, "a": 2}
    b = {"a": 2, "b": 1}
    assert canonical_dumps(a) == canonical_dumps(b)


def test_sha256_json_is_stable():
    a = {"b": 1, "a": 2}
    b = {"a": 2, "b": 1}
    assert sha256_json(a) == sha256_json(b)
