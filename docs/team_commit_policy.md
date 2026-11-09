# Team Git Commit Policy

## 1. Overview and Purpose

This policy outlines the Git commit expectations and milestone review criteria for the AtmoSync Dashboard project during the internship. All team members must adhere to these guidelines to ensure consistent project momentum, transparent peer visibility, and successful completion of official internship reviews.

---

## 2. Official Internship Review Requirements

Official internship performance evaluations are governed by specific commit activity thresholds. Reviewers inspect Git history to verify continuous engagement.

### Mid Review
* **Review Window:** 20th–27th of the current month.
* **Official Requirement:** A minimum of **10 distinct qualifying commit days** within the **preceding 14 days** prior to the review date.

### Final Review
* **Review Window:** 5th–10th of the following month.
* **Official Requirement:** At least **one qualifying commit every day** across the **preceding 20 days** prior to the review date (20 distinct qualifying commit days).

---

## 3. Understanding Commits vs. Distinct Commit Days

It is essential to understand how activity is tracked:

* **Commit Count:** The total number of Git commits made. For example, pushing five commits in a single evening equals five commits.
* **Distinct Commit Day:** A distinct calendar day (in local time / UTC as logged by Git author date) on which at least one substantive, qualifying commit was created.

> [!IMPORTANT]
> **Batching commits on a single day does not satisfy review requirements.**  
> Pushing 10 commits on a single Sunday counts as only **one (1) commit day**. Review criteria evaluate regularity over time, not isolated bursts of activity. Work must be planned and committed across separate days.

---

## 4. Qualifying Commits

A **qualifying commit** must demonstrate genuine, meaningful progress on an assigned component:

* Meaningful code implementations, refactoring, or bug fixes.
* Test suite additions or documentation updates.
* Valid configuration or data model files.

**Non-Qualifying Commits:**
* Empty commits (`git commit --allow-empty`).
* Trivial cosmetic changes (e.g., adding a single blank line or fixing a typo across several commits to inflate activity).
* Automated or bot-generated noise.

Every commit must be functional, relevant, and described using a clear commit message.

---

## 5. Official Requirements vs. Internal Team Recommendations

To ensure smooth collaboration, we distinguish formal evaluation gates from internal best practices:

| Category | Official Review Requirements | Internal Team Recommendation |
|---|---|---|
| **Mid Review** | Min. 10 distinct commit days in the preceding 14 days | Commit meaningful progress on your feature branch daily |
| **Final Review** | 1 qualifying commit every day in the preceding 20 days | Complete and test components incrementally rather than in bulk |
| **Daily Routine** | Enforced at official milestone windows | Daily push before ending the workday to keep teammate dependencies unblocked |

### Internal Recommendation: Aim for Daily Meaningful Contributions
While the official review windows enforce specific 14-day and 20-day windows, our internal team recommendation is for every member (**Roshan**, **Sam**, **Nisha**, and **Navya**) to aim for consistent daily meaningful contributions on their respective branch. 

Consistent daily commits:
1. Prevent last-minute panic before the mid and final review windows.
2. Allow teammates to integrate dependencies early and spot compatibility issues.
3. Provide a clear, auditable timeline of how your assigned module evolved.

