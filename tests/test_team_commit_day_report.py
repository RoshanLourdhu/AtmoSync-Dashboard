"""
Automated Tests for scripts/team_commit_day_report.py.

Uses isolated temporary Git repositories (with bare remotes) so tests are
100% deterministic and never depend on live team Git history.
"""
from __future__ import annotations

import csv
import io
import json
import subprocess
from datetime import date
from pathlib import Path

import pytest

from scripts.team_commit_day_report import (
    CommitRecord,
    TeamMemberConfig,
    generate_team_audit_report,
    main,
    render_csv_report,
    render_markdown_report,
    render_terminal_report,
)


def _git(cwd: Path, *args: str, env_extra: dict[str, str] | None = None) -> str:
    """Run a git command in cwd and raise on failure."""
    import os

    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _commit_file(
    repo: Path,
    filename: str,
    content: str,
    author_name: str,
    author_email: str,
    iso_timestamp: str,
    message: str,
) -> str:
    """Create/update a file and commit it with explicit author and timestamp."""
    target = repo / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    _git(repo, "add", filename)
    _git(
        repo,
        "commit",
        "-m",
        message,
        env_extra={
            "GIT_AUTHOR_NAME": author_name,
            "GIT_AUTHOR_EMAIL": author_email,
            "GIT_AUTHOR_DATE": iso_timestamp,
            "GIT_COMMITTER_NAME": author_name,
            "GIT_COMMITTER_EMAIL": author_email,
            "GIT_COMMITTER_DATE": iso_timestamp,
        },
    )
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture()
def temp_team_repo(tmp_path: Path) -> tuple[Path, Path, list[TeamMemberConfig]]:
    """Create a temporary bare remote + working clone with main and member branches."""
    remote_dir = tmp_path / "remote.git"
    work_dir = tmp_path / "work"
    _git(tmp_path, "init", "--bare", str(remote_dir))
    _git(tmp_path, "clone", str(remote_dir), str(work_dir))

    # Configure fallback user for merges
    _git(work_dir, "config", "user.name", "Repo Admin")
    _git(work_dir, "config", "user.email", "admin@example.com")

    # Initial main commit (shared ancestor)
    _git(work_dir, "checkout", "-b", "main")
    _commit_file(
        work_dir,
        "README.md",
        "# Temp Repo\n",
        "Roshan",
        "roshan@example.com",
        "2026-11-01T10:00:00+05:30",
        "chore: initial main commit",
    )
    _git(work_dir, "push", "-u", "origin", "main")

    members = [
        TeamMemberConfig(name="Roshan", branch="roshan", emails=("roshan@example.com",)),
        TeamMemberConfig(name="Sam Roshan", branch="sam-roshan", emails=("sam@example.com",)),
        TeamMemberConfig(name="Nisha", branch="nisha", emails=("nisha@example.com",)),
        TeamMemberConfig(name="Navya", branch="navya", emails=("navya@example.com",)),
    ]
    return work_dir, remote_dir, members


def test_multiple_commits_same_date_count_as_one_day(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """Multiple commits on a single calendar date must count as 1 unique contribution day."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    for idx in range(5):
        _commit_file(
            work_dir,
            f"f_{idx}.txt",
            f"data {idx}\n",
            "Roshan",
            "roshan@example.com",
            f"2026-11-20T1{idx}:00:00+05:30",
            f"feat: work {idx}",
        )
    _git(work_dir, "push", "-u", "origin", "roshan")

    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 20),
        end_date=date(2026, 11, 22),
        perform_fetch=True,
    )
    res = report.member_results[0]
    assert res.total_qualifying_commits == 5
    assert res.unique_contribution_days == 1
    assert res.contribution_dates == [date(2026, 11, 20)]


def test_commits_on_three_separate_dates_count_as_three_days(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """5 commits on Nov 20, 3 on Nov 21, and 7 on Nov 22 -> 15 commits, 3 unique days."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    counts = [(date(2026, 11, 20), 5), (date(2026, 11, 21), 3), (date(2026, 11, 22), 7)]
    seq = 0
    for d, n in counts:
        for i in range(n):
            seq += 1
            _commit_file(
                work_dir,
                f"item_{seq}.txt",
                f"v{seq}\n",
                "Roshan",
                "roshan@example.com",
                f"{d.isoformat()}T10:{i:02d}:00+05:30",
                f"feat: commit {seq}",
            )
    _git(work_dir, "push", "-u", "origin", "roshan")

    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 20),
        end_date=date(2026, 11, 22),
    )
    res = report.member_results[0]
    assert res.total_qualifying_commits == 15
    assert res.unique_contribution_days == 3
    assert res.contribution_dates == [date(2026, 11, 20), date(2026, 11, 21), date(2026, 11, 22)]


def test_different_members_counted_independently_and_team_union(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """Each member is counted independently and team total is the union of dates, not the sum."""
    work_dir, _, members = temp_team_repo

    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    _commit_file(work_dir, "r1.txt", "1", "Roshan", "roshan@example.com", "2026-11-10T12:00:00+05:30", "r1")
    _commit_file(work_dir, "r2.txt", "2", "Roshan", "roshan@example.com", "2026-11-11T12:00:00+05:30", "r2")
    _git(work_dir, "push", "-u", "origin", "roshan")

    _git(work_dir, "checkout", "-b", "sam-roshan", "origin/main")
    _commit_file(work_dir, "s1.txt", "1", "Sam Roshan", "sam@example.com", "2026-11-11T14:00:00+05:30", "s1")
    _git(work_dir, "push", "-u", "origin", "sam-roshan")

    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:2],
        start_date=date(2026, 11, 10),
        end_date=date(2026, 11, 12),
    )
    assert report.member_results[0].unique_contribution_days == 2
    assert report.member_results[1].unique_contribution_days == 1
    assert report.total_unique_team_dates == 2
    assert report.all_members_contributed_dates == [date(2026, 11, 11)]
    assert report.incomplete_team_dates == [date(2026, 11, 10), date(2026, 11, 12)]


def test_other_author_commits_on_member_branch_excluded(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """Commits authored by another team member on Navya's branch must not count for Navya."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "navya", "origin/main")
    _commit_file(work_dir, "n1.txt", "1", "Navya", "navya@example.com", "2026-11-15T11:00:00+05:30", "navya work")
    _commit_file(work_dir, "r_help.txt", "2", "Roshan", "roshan@example.com", "2026-11-16T11:00:00+05:30", "roshan delegated")
    _git(work_dir, "push", "-u", "origin", "navya")

    navya = members[3]
    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members,
        start_date=date(2026, 11, 15),
        end_date=date(2026, 11, 16),
    )
    navya_res = next(r for r in report.member_results if r.member_name == navya.name)
    assert navya_res.total_qualifying_commits == 1
    assert navya_res.unique_contribution_days == 1
    assert navya_res.contribution_dates == [date(2026, 11, 15)]
    assert len(navya_res.other_member_commits) == 1
    assert navya_res.other_member_commits[0].author_email == "roshan@example.com"


def test_shared_main_branch_commits_not_counted(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """Commits already reachable from origin/main must not be counted as branch-specific work."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "main")
    _commit_file(
        work_dir,
        "shared.txt",
        "shared",
        "Roshan",
        "roshan@example.com",
        "2026-11-10T10:00:00+05:30",
        "shared main commit",
    )
    _git(work_dir, "push", "origin", "main")

    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    _commit_file(
        work_dir,
        "branch_only.txt",
        "branch",
        "Roshan",
        "roshan@example.com",
        "2026-11-12T10:00:00+05:30",
        "branch-only commit",
    )
    _git(work_dir, "push", "-u", "origin", "roshan")

    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 10),
        end_date=date(2026, 11, 12),
    )
    res = report.member_results[0]
    assert res.total_qualifying_commits == 1
    assert res.contribution_dates == [date(2026, 11, 12)]


def test_timezone_conversion_shifts_calendar_date(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """A commit at 2026-11-20T20:00:00+00:00 is Nov 20 in UTC but Nov 21 in Asia/Kolkata."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    _commit_file(
        work_dir,
        "tz.txt",
        "tz",
        "Roshan",
        "roshan@example.com",
        "2026-11-20T20:00:00+00:00",
        "late utc commit",
    )
    _git(work_dir, "push", "-u", "origin", "roshan")

    ist_report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 20),
        end_date=date(2026, 11, 21),
        timezone_name="Asia/Kolkata",
    )
    utc_report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 20),
        end_date=date(2026, 11, 21),
        timezone_name="UTC",
    )
    assert ist_report.member_results[0].contribution_dates == [date(2026, 11, 21)]
    assert utc_report.member_results[0].contribution_dates == [date(2026, 11, 20)]


def test_start_and_end_date_boundaries_are_inclusive(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """Commits on exact start and end dates are included; outside dates are excluded."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    for d in ["2026-11-09", "2026-11-10", "2026-11-12", "2026-11-13"]:
        _commit_file(
            work_dir,
            f"d_{d}.txt",
            d,
            "Roshan",
            "roshan@example.com",
            f"{d}T12:00:00+05:30",
            f"commit {d}",
        )
    _git(work_dir, "push", "-u", "origin", "roshan")

    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 10),
        end_date=date(2026, 11, 12),
    )
    res = report.member_results[0]
    assert res.contribution_dates == [date(2026, 11, 10), date(2026, 11, 12)]
    assert res.missing_dates == [date(2026, 11, 11)]


def test_duplicate_commit_hashes_not_counted_twice(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate commit hashes in parsed log output must be deduplicated."""
    from scripts import team_commit_day_report as mod

    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    h1 = _commit_file(
        work_dir,
        "dup.txt",
        "dup",
        "Roshan",
        "roshan@example.com",
        "2026-11-18T10:00:00+05:30",
        "single commit",
    )
    _git(work_dir, "push", "-u", "origin", "roshan")

    # Simulate git log returning the same commit hash twice
    sep = mod.FIELD_SEP
    rec = mod.RECORD_SEP
    line = f"{h1}{sep}{sep}Roshan{sep}roshan@example.com{sep}2026-11-18T10:00:00+05:30{sep}single commit{rec}"
    orig_run_git = mod._run_git

    def fake_run_git(repo_path: Path, args: list[str] | tuple[str, ...]):
        if args and args[0] == "log":
            return subprocess.CompletedProcess(args=["git", *args], returncode=0, stdout=line + line, stderr="")
        return orig_run_git(repo_path, args)

    monkeypatch.setattr(mod, "_run_git", fake_run_git)

    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 18),
        end_date=date(2026, 11, 18),
        perform_fetch=False,
    )
    res = report.member_results[0]
    assert res.total_qualifying_commits == 1
    assert res.unique_contribution_days == 1


def test_merge_commit_exclusion_and_inclusion(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """Merge-only commits are excluded by default and included when include_merges=True."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    _commit_file(
        work_dir,
        "base.txt",
        "base",
        "Roshan",
        "roshan@example.com",
        "2026-11-14T10:00:00+05:30",
        "base work",
    )
    # Create a side branch with an older commit and merge it with --no-ff on Nov 15
    _git(work_dir, "checkout", "-b", "side-feature")
    _commit_file(
        work_dir,
        "side.txt",
        "side",
        "Roshan",
        "roshan@example.com",
        "2026-11-14T11:00:00+05:30",
        "side work",
    )
    _git(work_dir, "checkout", "roshan")
    _git(
        work_dir,
        "merge",
        "--no-ff",
        "side-feature",
        "-m",
        "Merge side-feature",
        env_extra={
            "GIT_AUTHOR_NAME": "Roshan",
            "GIT_AUTHOR_EMAIL": "roshan@example.com",
            "GIT_AUTHOR_DATE": "2026-11-15T15:00:00+05:30",
            "GIT_COMMITTER_NAME": "Roshan",
            "GIT_COMMITTER_EMAIL": "roshan@example.com",
            "GIT_COMMITTER_DATE": "2026-11-15T15:00:00+05:30",
        },
    )
    _git(work_dir, "push", "-u", "origin", "roshan")

    excluded_report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 14),
        end_date=date(2026, 11, 15),
        include_merges=False,
    )
    ex_res = excluded_report.member_results[0]
    assert ex_res.excluded_merge_commits == 1
    assert ex_res.unique_contribution_days == 1
    assert ex_res.contribution_dates == [date(2026, 11, 14)]

    included_report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 14),
        end_date=date(2026, 11, 15),
        include_merges=True,
    )
    in_res = included_report.member_results[0]
    assert in_res.excluded_merge_commits == 0
    assert in_res.unique_contribution_days == 2
    assert in_res.contribution_dates == [date(2026, 11, 14), date(2026, 11, 15)]


def test_missing_remote_branch_reported_correctly(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """Missing remote branches must be flagged unavailable instead of substituting another branch."""
    work_dir, _, members = temp_team_repo
    report = generate_team_audit_report(
        repo_path=work_dir,
        members=[members[2]],  # nisha branch was never created
        start_date=date(2026, 11, 10),
        end_date=date(2026, 11, 11),
    )
    res = report.member_results[0]
    assert res.branch_available is False
    assert res.total_qualifying_commits == 0
    assert res.unique_contribution_days == 0
    assert res.missing_dates == [date(2026, 11, 10), date(2026, 11, 11)]
    assert any("does not exist" in note for note in res.notes)


def test_unrecognized_author_identities_reported_separately(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """Commits by unknown authors must be reported under unrecognized_commits and not counted."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    _commit_file(
        work_dir,
        "unknown.txt",
        "x",
        "Mystery Bot",
        "mystery@unknown.dev",
        "2026-11-19T09:00:00+05:30",
        "unrecognized commit",
    )
    _git(work_dir, "push", "-u", "origin", "roshan")

    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 19),
        end_date=date(2026, 11, 19),
    )
    res = report.member_results[0]
    assert res.total_qualifying_commits == 0
    assert res.unique_contribution_days == 0
    assert len(res.unrecognized_commits) == 1
    assert res.unrecognized_commits[0].author_email == "mystery@unknown.dev"


def test_empty_reporting_period_produces_zero_day_result(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
) -> None:
    """A valid period with zero matching commits returns zero days and full missing_dates."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    _commit_file(
        work_dir,
        "early.txt",
        "early",
        "Roshan",
        "roshan@example.com",
        "2026-11-05T10:00:00+05:30",
        "early commit",
    )
    _git(work_dir, "push", "-u", "origin", "roshan")

    report = generate_team_audit_report(
        repo_path=work_dir,
        members=members[:1],
        start_date=date(2026, 11, 20),
        end_date=date(2026, 11, 21),
    )
    res = report.member_results[0]
    assert res.branch_available is True
    assert res.total_qualifying_commits == 0
    assert res.unique_contribution_days == 0
    assert res.first_contribution_date is None
    assert res.latest_contribution_date is None
    assert res.missing_dates == [date(2026, 11, 20), date(2026, 11, 21)]
    assert report.total_unique_team_dates == 0


def test_cli_markdown_and_csv_outputs(
    temp_team_repo: tuple[Path, Path, list[TeamMemberConfig]],
    tmp_path: Path,
) -> None:
    """CLI generates valid terminal, Markdown, and CSV reports with custom config."""
    work_dir, _, members = temp_team_repo
    _git(work_dir, "checkout", "-b", "roshan", "origin/main")
    _commit_file(
        work_dir,
        "cli.txt",
        "cli",
        "Roshan",
        "roshan@example.com",
        "2026-11-22T12:00:00+05:30",
        "cli test",
    )
    _git(work_dir, "push", "-u", "origin", "roshan")

    cfg_path = tmp_path / "team.json"
    cfg_path.write_text(
        json.dumps([{"name": "Roshan", "branch": "roshan", "emails": ["roshan@example.com"]}]),
        encoding="utf-8",
    )
    md_path = tmp_path / "report.md"
    csv_path = tmp_path / "report.csv"

    rc = main([
        "--repo", str(work_dir),
        "--config", str(cfg_path),
        "--start", "2026-11-22",
        "--end", "2026-11-22",
        "--markdown", str(md_path),
        "--csv", str(csv_path),
        "--quiet",
    ])
    assert rc == 0
    assert "# AtmoSync Team Contribution-Day Audit Report" in md_path.read_text(encoding="utf-8")

    rows = list(csv.DictReader(io.StringIO(csv_path.read_text(encoding="utf-8"))))
    assert len(rows) == 1
    assert rows[0]["member_name"] == "Roshan"
    assert rows[0]["unique_contribution_days"] == "1"
    assert rows[0]["contribution_dates"] == "2026-11-22"
