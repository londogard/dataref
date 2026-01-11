from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .repo import Repo

app = typer.Typer(
    add_completion=False, help="strand: client-driven data versioning for S3"
)
console = Console()


@app.command()
def init(
    root: str = typer.Argument(..., help="Repo root (e.g. s3://bucket/prefix or /path)")
):
    """Initialize a strand repository at root."""
    repo = Repo.init(root)
    console.print(f"Initialized strand repo at [bold]{repo.config.root}[/bold]")


@app.command()
def commit(
    root: str = typer.Argument(
        ..., help="Repo root (e.g. s3://bucket/prefix or /path)"
    ),
    message: str = typer.Option(..., "-m", "--message", help="Commit message"),
    author: Optional[str] = typer.Option(None, "--author", help="Author string"),
):
    """Create a commit (metadata-only MVP)."""
    repo = Repo.open(root)
    commit_id = repo.commit(message=message, author=author)
    console.print(f"Committed [bold]{commit_id}[/bold] on {repo.current_branch()}")


@app.command()
def log(
    root: str = typer.Argument(..., help="Repo root"),
    limit: int = typer.Option(20, "--limit", min=1, max=200),
):
    """Show commit history."""
    repo = Repo.open(root)
    commits = repo.log(limit=limit)

    table = Table(title=f"strand log ({repo.current_branch()})")
    table.add_column("message")
    table.add_column("author")
    table.add_column("created_at")
    table.add_column("parent")

    for c in commits:
        table.add_row(
            c.message,
            c.author or "",
            c.created_at.isoformat(),
            (c.parent or "")[:12],
        )

    console.print(table)


@app.command("branch")
def branch_cmd(
    root: str = typer.Argument(..., help="Repo root"),
    name: str = typer.Argument(..., help="Branch name"),
    from_commit: Optional[str] = typer.Option(
        None, "--from", help="Commit id to branch from"
    ),
):
    """Create a branch."""
    repo = Repo.open(root)
    repo.create_branch(name=name, from_commit=from_commit)
    console.print(f"Created branch [bold]{name}[/bold]")


@app.command()
def checkout(
    root: str = typer.Argument(..., help="Repo root"),
    name: str = typer.Argument(..., help="Branch name"),
):
    """Checkout a branch."""
    repo = Repo.open(root)
    repo.checkout_branch(name)
    console.print(f"Checked out [bold]{name}[/bold]")


@app.command()
def snapshot(
    root: str = typer.Argument(..., help="Repo root"),
    dataset: str = typer.Argument(
        ..., help="Dataset root to snapshot (local path or s3://...)"
    ),
    message: Optional[str] = typer.Option(
        None, "-m", "--message", help="Commit message"
    ),
    author: Optional[str] = typer.Option(None, "--author", help="Author string"),
):
    """Create a snapshot commit for a dataset root."""
    repo = Repo.open(root)
    commit_id = repo.snapshot(dataset_root=dataset, message=message, author=author)
    console.print(f"Snapshotted [bold]{dataset}[/bold] as [bold]{commit_id}[/bold]")


@app.command()
def diff(
    root: str = typer.Argument(..., help="Repo root"),
    from_commit: Optional[str] = typer.Option(
        None, "--from", help="From commit id (default: HEAD parent)"
    ),
    to_commit: Optional[str] = typer.Option(
        None, "--to", help="To commit id (default: HEAD)"
    ),
    limit: int = typer.Option(
        50, "--limit", min=1, max=500, help="Max rows per section"
    ),
):
    """Diff two snapshot commits (file-level)."""
    repo = Repo.open(root)

    head = repo.head_commit()
    if not head:
        raise typer.BadParameter("No commits on current branch")

    to_id = to_commit or head
    to_obj = repo.get_commit(to_id)
    from_id = from_commit or to_obj.parent
    if not from_id:
        raise typer.BadParameter("No parent commit to diff from; pass --from")

    d = repo.diff_snapshots(from_id, to_id)

    console.print(
        f"Diff {d['from'][:12]} -> {d['to'][:12]}\n"
        f"dataset: {d['dataset_from']} -> {d['dataset_to']}"
    )

    def render(title: str, items: list[str]):
        table = Table(title=title)
        table.add_column("path")
        for p in items[:limit]:
            table.add_row(p)
        if len(items) > limit:
            table.add_row(f"... ({len(items) - limit} more)")
        console.print(table)

    render(f"added ({len(d['added'])})", d["added"])
    render(f"removed ({len(d['removed'])})", d["removed"])
    render(f"modified ({len(d['modified'])})", d["modified"])


def main() -> None:
    app()


if __name__ == "__main__":
    main()
