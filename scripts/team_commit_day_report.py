#!/usr/bin/env python3
"""
AtmoSync Team GitHub Contribution-Day Audit Tool.

Calculates how many DISTINCT calendar days each team member has committed to
their respective GitHub branch, using branch-specific reachability
(<base_branch>..<remote>/<branch>), author identity matching, timezone
normalization, and configurable merge-commit handling.

Disclaimer:
    This script implements an independent Git history auditing convention
    for team lead visibility. It may not exactly reproduce GitHub's official
    contribution graph or Infotact's official internship eligibility rules.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_TIMEZONE = "Asia/Kolkata"
DEFAULT_REMOTE = "origin"
DEFAULT_BASE_BRANCH = "origin/main"
FIELD_SEP = "\x1f"
RECORD_SEP = "\x1e"


@dataclass(frozen=True)
class TeamMemberConfig:
    """Configuration mapping a team member to their Git branch and author identities."""

    name: str
    branch: str
    emails: tuple[str, ...] = ()
    names: tuple[str, ...] = ()

    def matches_author(self, author_name: str, author_email: str) -> bool:
        """Return True if the commit author matches this member's configured identity."""
        norm_email = author_email.strip().lower()
        if self.emails and norm_email in {e.strip().lower() for e in self.emails}:
            return True
        norm_name = author_name.strip().lower()
        if self.names and norm_name in {n.strip().lower() for n in self.names}:
            return True
        return False


DEFAULT_TEAM_CONFIG: tuple[TeamMemberConfig, ...] = (
    TeamMemberConfig(
        name="Roshan",
        branch="roshan",
        emails=("roshanbtech2023@gmail.com",),
        names=("Roshan",),
    ),
    TeamMemberConfig(
        name="Sam Roshan",
        branch="sam-roshan",
        emails=("sam.roshan05020@gmail.com",),
        names=("Sam Roshan", "Sam Roshan C"),
    ),
    TeamMemberConfig(
        name="Nisha",
        branch="nisha",
        emails=("nishagovintharasan2004@gmail.com",),
        names=("Nisha Govintharasan", "Nisha-200408", "NISHA23", "Nisha"),
    ),
    TeamMemberConfig(
        name="Navya",
        branch="navya",
        emails=("navya@gmail.com",),
        names=("Navya",),
    ),
)


@dataclass(frozen=True)
class CommitRecord:
    """Normalized representation of a single Git commit."""

    commit_hash: str
    parent_hashes: tuple[str, ...]
    author_name: str
    author_email: str
    author_timestamp: str
    subject: str
    local_date: date

    @property
    def is_merge(self) -> bool:
        """Return True when the commit has more than one parent."""
        return len(self.parent_hashes) > 1


@dataclass
class MemberAuditResult:
    """Contribution-day audit results for a single team member."""

    member_name: str
    branch_name: str
    remote_ref: str
    branch_available: bool
    fetch_status: str
    total_qualifying_commits: int
    unique_contribution_days: int
    contribution_dates: list[date] = field(default_factory=list)
    first_contribution_date: date | None = None
    latest_contribution_date: date | None = None
    missing_dates: list[date] = field(default_factory=list)
    unrecognized_commits: list[CommitRecord] = field(default_factory=list)
    other_member_commits: list[CommitRecord] = field(default_factory=list)
    excluded_merge_commits: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass
class TeamAuditReport:
    """Complete team contribution-day audit report across a reporting period."""

    repo_path: Path
    remote: str
    base_branch: str | None
    timezone: str
    start_date: date
    end_date: date
    include_merges: bool
    fetch_status: str
    member_results: list[MemberAuditResult]
    total_unique_team_dates: int
    team_contribution_dates: list[date]
    all_members_contributed_dates: list[date]
    incomplete_team_dates: list[date]
    generated_at: str


def load_team_config(config_path: Path | None) -> list[TeamMemberConfig]:
    """Load team member configuration from a JSON file or return the default mapping."""
    if config_path is None:
        return list(DEFAULT_TEAM_CONFIG)

    raw = json.loads(config_path.read_text(encoding="utf-8"))
    members_raw = raw.get("members", raw) if isinstance(raw, dict) else raw
    if not isinstance(members_raw, list) or not members_raw:
        raise ValueError("Team config must be a non-empty list or {'members': [...]} object.")

    configs: list[TeamMemberConfig] = []
    for entry in members_raw:
        configs.append(
            TeamMemberConfig(
                name=str(entry["name"]),
                branch=str(entry["branch"]),
                emails=tuple(str(e) for e in entry.get("emails", ())),
                names=tuple(str(n) for n in entry.get("names", ())),
            )
        )
    return configs


def _run_git(repo_path: Path, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Execute a git command in repo_path and return the completed process."""
    return subprocess.run(
        ["git", *args],
        cwd=str(repo_path),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def fetch_remote(repo_path: Path, remote: str) -> str:
    """Fetch latest refs from the configured Git remote and return status text."""
    proc = _run_git(repo_path, ["fetch", remote, "--prune"])
    if proc.returncode == 0:
        return f"fetched ({remote})"
    err = (proc.stderr or proc.stdout).strip().splitlines()
    reason = err[-1] if err else f"exit code {proc.returncode}"
    return f"fetch_failed ({reason})"


def _ref_exists(repo_path: Path, ref: str) -> bool:
    """Return True if the given Git ref resolves to a valid object."""
    proc = _run_git(repo_path, ["rev-parse", "--verify", "--quiet", ref])
    return proc.returncode == 0


def _iter_dates_inclusive(start_date: date, end_date: date) -> list[date]:
    """Return all calendar dates from start_date to end_date inclusive."""
    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(total_days)]


def _parse_git_log(
    repo_path: Path,
    rev_range: str,
    tz: ZoneInfo,
) -> list[CommitRecord]:
    """Read commits from git log for rev_range and convert timestamps to tz."""
    fmt = f"%H{FIELD_SEP}%P{FIELD_SEP}%an{FIELD_SEP}%ae{FIELD_SEP}%aI{FIELD_SEP}%s{RECORD_SEP}"
    proc = _run_git(repo_path, ["log", rev_range, f"--format={fmt}"])
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout).strip()
        raise RuntimeError(f"git log failed for '{rev_range}': {err}")

    records: list[CommitRecord] = []
    seen_hashes: set[str] = set()
    for raw_record in proc.stdout.split(RECORD_SEP):
        cleaned = raw_record.strip("\r\n")
        if not cleaned:
            continue
        parts = cleaned.split(FIELD_SEP, 5)
        if len(parts) < 6:
            continue
        commit_hash, parents_str, author_name, author_email, author_ts, subject = parts
        commit_hash = commit_hash.strip()
        if not commit_hash or commit_hash in seen_hashes:
            continue
        seen_hashes.add(commit_hash)

        dt = datetime.fromisoformat(author_ts.strip())
        local_date = dt.astimezone(tz).date()
        parent_hashes = tuple(p for p in parents_str.strip().split() if p)
        records.append(
            CommitRecord(
                commit_hash=commit_hash,
                parent_hashes=parent_hashes,
                author_name=author_name.strip(),
                author_email=author_email.strip(),
                author_timestamp=author_ts.strip(),
                subject=subject.strip(),
                local_date=local_date,
            )
        )
    return records


def audit_member_branch(
    repo_path: Path,
    member: TeamMemberConfig,
    all_members: Sequence[TeamMemberConfig],
    remote: str,
    base_branch: str | None,
    start_date: date,
    end_date: date,
    tz: ZoneInfo,
    include_merges: bool,
    fetch_status: str,
) -> MemberAuditResult:
    """Audit a single team member's remote branch for distinct contribution days."""
    remote_ref = f"{remote}/{member.branch}" if remote else member.branch
    period_dates = _iter_dates_inclusive(start_date, end_date)

    if not _ref_exists(repo_path, remote_ref):
        return MemberAuditResult(
            member_name=member.name,
            branch_name=member.branch,
            remote_ref=remote_ref,
            branch_available=False,
            fetch_status=fetch_status,
            total_qualifying_commits=0,
            unique_contribution_days=0,
            missing_dates=period_dates,
            notes=[f"Branch '{remote_ref}' does not exist or could not be resolved."],
        )

    notes: list[str] = []
    if base_branch:
        if not _ref_exists(repo_path, base_branch):
            return MemberAuditResult(
                member_name=member.name,
                branch_name=member.branch,
                remote_ref=remote_ref,
                branch_available=False,
                fetch_status=fetch_status,
                total_qualifying_commits=0,
                unique_contribution_days=0,
                missing_dates=period_dates,
                notes=[f"Base branch '{base_branch}' does not exist or could not be resolved."],
            )
        rev_range = f"{base_branch}..{remote_ref}"
    else:
        rev_range = remote_ref

    commits = _parse_git_log(repo_path, rev_range, tz)
    other_members = [m for m in all_members if m.name != member.name]

    qualifying_dates: set[date] = set()
    qualifying_count = 0
    excluded_merges = 0
    unrecognized: list[CommitRecord] = []
    other_member_commits: list[CommitRecord] = []

    for commit in commits:
        if not (start_date <= commit.local_date <= end_date):
            continue

        if commit.is_merge and not include_merges:
            excluded_merges += 1
            continue

        if member.matches_author(commit.author_name, commit.author_email):
            qualifying_count += 1
            qualifying_dates.add(commit.local_date)
        elif any(other.matches_author(commit.author_name, commit.author_email) for other in other_members):
            other_member_commits.append(commit)
        else:
            unrecognized.append(commit)

    sorted_dates = sorted(qualifying_dates)
    missing_dates = [d for d in period_dates if d not in qualifying_dates]

    if other_member_commits:
        notes.append(
            f"Excluded {len(other_member_commits)} commit(s) authored by another team member on '{remote_ref}'."
        )
    if excluded_merges:
        notes.append(f"Excluded {excluded_merges} merge commit(s) per default merge policy.")

    return MemberAuditResult(
        member_name=member.name,
        branch_name=member.branch,
        remote_ref=remote_ref,
        branch_available=True,
        fetch_status=fetch_status,
        total_qualifying_commits=qualifying_count,
        unique_contribution_days=len(sorted_dates),
        contribution_dates=sorted_dates,
        first_contribution_date=sorted_dates[0] if sorted_dates else None,
        latest_contribution_date=sorted_dates[-1] if sorted_dates else None,
        missing_dates=missing_dates,
        unrecognized_commits=unrecognized,
        other_member_commits=other_member_commits,
        excluded_merge_commits=excluded_merges,
        notes=notes,
    )


def generate_team_audit_report(
    repo_path: Path,
    members: Sequence[TeamMemberConfig] | None = None,
    remote: str = DEFAULT_REMOTE,
    base_branch: str | None = DEFAULT_BASE_BRANCH,
    start_date: date | None = None,
    end_date: date | None = None,
    timezone_name: str = DEFAULT_TIMEZONE,
    include_merges: bool = False,
    perform_fetch: bool = True,
) -> TeamAuditReport:
    """Run the full team contribution-day audit and return a structured report."""
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone '{timezone_name}'.") from exc

    resolved_end = end_date or datetime.now(tz).date()
    resolved_start = start_date or (resolved_end - timedelta(days=20))
    if resolved_start > resolved_end:
        raise ValueError(
            f"Invalid reporting period: start date ({resolved_start}) is after end date ({resolved_end})."
        )

    member_list = list(members) if members is not None else list(DEFAULT_TEAM_CONFIG)
    fetch_status = fetch_remote(repo_path, remote) if (perform_fetch and remote) else "skipped"

    results = [
        audit_member_branch(
            repo_path=repo_path,
            member=member,
            all_members=member_list,
            remote=remote,
            base_branch=base_branch,
            start_date=resolved_start,
            end_date=resolved_end,
            tz=tz,
            include_merges=include_merges,
            fetch_status=fetch_status,
        )
        for member in member_list
    ]

    period_dates = _iter_dates_inclusive(resolved_start, resolved_end)
    team_dates_union: set[date] = set()
    for res in results:
        team_dates_union.update(res.contribution_dates)
    sorted_team_dates = sorted(team_dates_union)

    member_date_sets = [set(res.contribution_dates) for res in results]
    if member_date_sets:
        all_contributed = [
            d for d in period_dates if all(d in date_set for date_set in member_date_sets)
        ]
        incomplete_dates = [
            d for d in period_dates if not all(d in date_set for date_set in member_date_sets)
        ]
    else:
        all_contributed = []
        incomplete_dates = period_dates

    return TeamAuditReport(
        repo_path=repo_path,
        remote=remote,
        base_branch=base_branch,
        timezone=timezone_name,
        start_date=resolved_start,
        end_date=resolved_end,
        include_merges=include_merges,
        fetch_status=fetch_status,
        member_results=results,
        total_unique_team_dates=len(sorted_team_dates),
        team_contribution_dates=sorted_team_dates,
        all_members_contributed_dates=all_contributed,
        incomplete_team_dates=incomplete_dates,
        generated_at=datetime.now(tz).isoformat(timespec="seconds"),
    )


def _fmt_date(d: date | None) -> str:
    return d.isoformat() if d else "N/A"


def _fmt_dates(dates: Sequence[date]) -> str:
    return ", ".join(d.isoformat() for d in dates) if dates else "None"


def render_terminal_report(report: TeamAuditReport, show_dates: bool = True) -> str:
    """Format the audit report as a human-readable terminal summary."""
    merge_policy = "included" if report.include_merges else "excluded (default)"
    lines = [
        "=" * 78,
        "ATMOSYNC TEAM CONTRIBUTION-DAY AUDIT REPORT",
        "=" * 78,
        f"Reporting Period : {report.start_date.isoformat()} to {report.end_date.isoformat()} (inclusive)",
        f"Timezone         : {report.timezone}",
        f"Base Comparison  : {report.base_branch or 'None (full branch history)'}",
        f"Merge Commits    : {merge_policy}",
        f"Remote Fetch     : {report.fetch_status}",
        f"Generated At     : {report.generated_at}",
        "-" * 78,
        f"{'Member':<14} {'Branch':<13} {'Avail':<6} {'Commits':<8} {'Days':<6} {'First':<12} {'Latest':<12}",
        "-" * 78,
    ]

    for res in report.member_results:
        avail = "Yes" if res.branch_available else "NO"
        lines.append(
            f"{res.member_name:<14} {res.branch_name:<13} {avail:<6} "
            f"{res.total_qualifying_commits:<8} {res.unique_contribution_days:<6} "
            f"{_fmt_date(res.first_contribution_date):<12} {_fmt_date(res.latest_contribution_date):<12}"
        )

    lines.append("-" * 78)
    lines.append("MEMBER DETAILS")
    lines.append("-" * 78)

    for res in report.member_results:
        lines.append(f"[{res.member_name}] ({res.remote_ref}) | Branch available: {res.branch_available} | Fetch: {res.fetch_status}")
        if show_dates:
            lines.append(f"  Contribution dates ({res.unique_contribution_days}): {_fmt_dates(res.contribution_dates)}")
            lines.append(f"  Missing dates      ({len(res.missing_dates)}): {_fmt_dates(res.missing_dates)}")
        else:
            lines.append(f"  Unique days: {res.unique_contribution_days} | Missing days: {len(res.missing_dates)}")

        if res.unrecognized_commits:
            lines.append(f"  Unrecognized author commits ({len(res.unrecognized_commits)}):")
            for c in res.unrecognized_commits:
                lines.append(
                    f"    - {c.commit_hash[:7]} | {c.local_date.isoformat()} | {c.author_name} <{c.author_email}> | {c.subject}"
                )
        else:
            lines.append("  Unrecognized author commits: None")

        for note in res.notes:
            lines.append(f"  Note: {note}")

    lines.extend([
        "-" * 78,
        "TEAM SUMMARY",
        "-" * 78,
        f"Total unique team contribution dates : {report.total_unique_team_dates}",
        "Individual contribution-day totals   : "
        + ", ".join(f"{r.member_name}={r.unique_contribution_days}" for r in report.member_results),
        f"Dates where ALL members contributed  : {len(report.all_members_contributed_dates)}"
        + (f" ({_fmt_dates(report.all_members_contributed_dates)})" if show_dates and report.all_members_contributed_dates else ""),
        f"Dates missing >=1 member             : {len(report.incomplete_team_dates)}"
        + (f" ({_fmt_dates(report.incomplete_team_dates)})" if show_dates and report.incomplete_team_dates else ""),
        "-" * 78,
        "Note: This audit reflects configured branch/author/timezone rules and may not",
        "      match GitHub's contribution graph or official Infotact eligibility rules.",
        "=" * 78,
    ])
    return "\n".join(lines)


def render_markdown_report(report: TeamAuditReport, show_dates: bool = True) -> str:
    """Format the audit report as a GitHub-flavored Markdown document."""
    merge_policy = "Included" if report.include_merges else "Excluded (default)"
    lines = [
        "# AtmoSync Team Contribution-Day Audit Report",
        "",
        f"- **Reporting Period:** `{report.start_date.isoformat()}` to `{report.end_date.isoformat()}` (inclusive)",
        f"- **Timezone:** `{report.timezone}`",
        f"- **Base Comparison:** `{report.base_branch or 'None'}`",
        f"- **Merge Commits:** {merge_policy}",
        f"- **Remote Fetch Status:** `{report.fetch_status}`",
        f"- **Generated At:** `{report.generated_at}`",
        "",
        "## Member Overview",
        "",
        "| Member | Branch | Available | Qualifying Commits | Unique Days | First Date | Latest Date | Missing Days | Unrecognized Commits |",
        "|---|---|---|---:|---:|---|---|---:|---:|",
    ]

    for res in report.member_results:
        lines.append(
            f"| {res.member_name} | `{res.branch_name}` | {'Yes' if res.branch_available else 'No'} "
            f"| {res.total_qualifying_commits} | {res.unique_contribution_days} "
            f"| {_fmt_date(res.first_contribution_date)} | {_fmt_date(res.latest_contribution_date)} "
            f"| {len(res.missing_dates)} | {len(res.unrecognized_commits)} |"
        )

    if show_dates:
        lines.extend(["", "## Exact Dates & Exceptions", ""])
        for res in report.member_results:
            lines.append(f"### {res.member_name} (`{res.remote_ref}`)")
            lines.append(f"- **Branch Status:** {'Available' if res.branch_available else 'Unavailable'} ({res.fetch_status})")
            lines.append(f"- **Contribution Dates ({res.unique_contribution_days}):** {_fmt_dates(res.contribution_dates)}")
            lines.append(f"- **Missing Dates ({len(res.missing_dates)}):** {_fmt_dates(res.missing_dates)}")
            if res.unrecognized_commits:
                lines.append(f"- **Unrecognized Author Commits ({len(res.unrecognized_commits)}):**")
                for c in res.unrecognized_commits:
                    lines.append(
                        f"  - `{c.commit_hash[:7]}` ({c.local_date.isoformat()}) `{c.author_name} <{c.author_email}>` — {c.subject}"
                    )
            else:
                lines.append("- **Unrecognized Author Commits:** None")
            for note in res.notes:
                lines.append(f"- **Note:** {note}")
            lines.append("")

    lines.extend([
        "## Team Summary",
        "",
        f"- **Total Unique Team Contribution Dates:** {report.total_unique_team_dates}",
        "- **Individual Contribution-Day Totals:** "
        + ", ".join(f"{r.member_name}: {r.unique_contribution_days}" for r in report.member_results),
        f"- **Dates Where All Members Contributed ({len(report.all_members_contributed_dates)}):** {_fmt_dates(report.all_members_contributed_dates)}",
        f"- **Dates With One or More Missing Members ({len(report.incomplete_team_dates)}):** {_fmt_dates(report.incomplete_team_dates)}",
        "",
        "> **Disclaimer:** This report reflects this tool's branch-level Git history convention and may not match GitHub's official contribution graph or Infotact's official eligibility rules.",
    ])
    return "\n".join(lines) + "\n"


def render_csv_report(report: TeamAuditReport) -> str:
    """Format the per-member audit results as CSV."""
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "member_name",
        "branch_name",
        "remote_ref",
        "branch_available",
        "fetch_status",
        "start_date",
        "end_date",
        "timezone",
        "total_qualifying_commits",
        "unique_contribution_days",
        "first_contribution_date",
        "latest_contribution_date",
        "contribution_dates",
        "missing_dates_count",
        "missing_dates",
        "unrecognized_commits_count",
        "other_member_commits_count",
        "excluded_merge_commits",
    ])
    for res in report.member_results:
        writer.writerow([
            res.member_name,
            res.branch_name,
            res.remote_ref,
            res.branch_available,
            res.fetch_status,
            report.start_date.isoformat(),
            report.end_date.isoformat(),
            report.timezone,
            res.total_qualifying_commits,
            res.unique_contribution_days,
            _fmt_date(res.first_contribution_date),
            _fmt_date(res.latest_contribution_date),
            ";".join(d.isoformat() for d in res.contribution_dates),
            len(res.missing_dates),
            ";".join(d.isoformat() for d in res.missing_dates),
            len(res.unrecognized_commits),
            len(res.other_member_commits),
            res.excluded_merge_commits,
        ])
    return output.getvalue()


def build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Audit distinct calendar contribution days per team member branch."
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Path to the Git repository (default: project root).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional JSON file defining custom team member -> branch/email mappings.",
    )
    parser.add_argument(
        "--remote",
        default=DEFAULT_REMOTE,
        help=f"Git remote name to fetch and inspect (default: {DEFAULT_REMOTE}).",
    )
    parser.add_argument(
        "--base-branch",
        default=DEFAULT_BASE_BRANCH,
        help=f"Base branch excluded from individual counts (default: {DEFAULT_BASE_BRANCH}). Pass '' to disable.",
    )
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=None,
        help="Inclusive reporting start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=None,
        help="Inclusive reporting end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"IANA timezone for calendar date normalization (default: {DEFAULT_TIMEZONE}).",
    )
    parser.add_argument(
        "--include-merges",
        action="store_true",
        help="Include merge commits in contribution-day counts (excluded by default).",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Skip 'git fetch' before inspecting branches.",
    )
    parser.add_argument(
        "--hide-dates",
        action="store_true",
        help="Omit the explicit lists of contribution and missing dates.",
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        default=None,
        help="Write a Markdown report to the specified path.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Write a CSV report to the specified path.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress terminal report output (useful when writing only --markdown or --csv).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    members = load_team_config(args.config)
    base_branch = args.base_branch if args.base_branch else None
    report = generate_team_audit_report(
        repo_path=args.repo.resolve(),
        members=members,
        remote=args.remote,
        base_branch=base_branch,
        start_date=args.start,
        end_date=args.end,
        timezone_name=args.timezone,
        include_merges=args.include_merges,
        perform_fetch=not args.no_fetch,
    )

    show_dates = not args.hide_dates
    if not args.quiet:
        print(render_terminal_report(report, show_dates=show_dates))

    if args.markdown is not None:
        args.markdown.parent.mkdir(parents=True, exist_ok=True)
        args.markdown.write_text(
            render_markdown_report(report, show_dates=show_dates),
            encoding="utf-8",
        )

    if args.csv is not None:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        args.csv.write_text(render_csv_report(report), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
