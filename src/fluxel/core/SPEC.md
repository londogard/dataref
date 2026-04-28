# Spec (Human Authored)


## `flx add`

We add files using two (2) different modes:

1. `meta` which is a pure metadata transaction, we simply store location of file with a hash of name + size
2. `hash` which is a git-like transaction where we store the file as `hash[:2]/hash[2:]`
    - We're using `blake3` to calculate the hash

The action of adding also needs to store a Index.

We chose actively not to store the actions transpired (e.g. `mv`, `add`, `rm`, ..)

The `add` action is local until commited.

### Logic

O(1) and simply appends to a list of operations.

## `flx rm`

Removes, like `add` very much.

### Logic

O(1) simply appends to a list of operations.


## `flx commit`

Commits and saves all the added (or removed/moved) files to a set branch.

## `flx branch`

Branch is checked out on S3-level directly.

## `flx merge`

Merge is done on S3-level.

## `flx diff`

Diff should be quite fast, but is allowed to use analytical index (that's built on-demand).

## `flx index`

Build an analytical index on-demand that's using DuckDB.