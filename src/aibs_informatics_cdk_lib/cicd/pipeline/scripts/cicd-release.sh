#!/bin/bash

#################################################################
#                   CI/CD Release Script
#  Description:
#   Purpose of this script is to facilitate submit Pull Requests
#   from a source branch/commit to a destination branch.
#
# Input Environment Variables:
#
#   CICD_RELEASE_SOURCE_ENV_TYPE:
#       Environment Type of source branch
#   CICD_RELEASE_TARGET_ENV_TYPE:
#       Environment Type of source branch
#   CICD_RELEASE_TARGET_BRANCH:
#       Target branch to submit pull request into
#   CICD_RELEASE_REVIEWER:
#       Reviewers for the PR
#   CICD_RELEASE_REPOSITORY:
#       Source repository in owner/repo format (used for GitHub API calls)
#   CICD_RELEASE_CHECKLIST:
#       Newline-separated checklist items (optional). When empty, no
#       Checklist section is rendered.
#   CICD_RELEASE_EXTRA_SECTIONS_JSON:
#       JSON object mapping section-heading -> markdown-body for extra
#       PR-body sections (optional).
#   CICD_RELEASE_LLM_SUMMARY:
#       "true"/"false" -- when true, prepend a Bedrock-generated narrative
#       summary above the bullet list.
#   CICD_RELEASE_LLM_MODEL_ID:
#       Bedrock model ID used for the LLM summary.

###################################


export CICD_RELEASE_SOURCE_COMMIT=$CODEBUILD_RESOLVED_SOURCE_VERSION
export CICD_RELEASE_CANDIDATE_BRANCH="candidate/$CICD_RELEASE_TARGET_BRANCH"

echo "==> CI/CD Release Inputs:"
echo "==> CICD_RELEASE_SOURCE_ENV_TYPE = $CICD_RELEASE_SOURCE_ENV_TYPE"
echo "==> CICD_RELEASE_TARGET_ENV_TYPE = $CICD_RELEASE_TARGET_ENV_TYPE"
echo "==> CICD_RELEASE_SOURCE_COMMIT = $CICD_RELEASE_SOURCE_COMMIT"
echo "==> CICD_RELEASE_CANDIDATE_BRANCH = $CICD_RELEASE_CANDIDATE_BRANCH"
echo "==> CICD_RELEASE_TARGET_BRANCH = $CICD_RELEASE_TARGET_BRANCH"
echo "==> CICD_RELEASE_REVIEWER = $CICD_RELEASE_REVIEWER"
echo "==> CICD_RELEASE_REPOSITORY = $CICD_RELEASE_REPOSITORY"
echo "==> CICD_RELEASE_LLM_SUMMARY = $CICD_RELEASE_LLM_SUMMARY"
echo "==> CICD_RELEASE_LLM_MODEL_ID = $CICD_RELEASE_LLM_MODEL_ID"

export CICD_RELEASE_GIT_MESSAGE="$(git log -1 --pretty=%B)"
export CICD_RELEASE_GIT_AUTHOR="$(git log -1 --pretty=%an)"
export CICD_RELEASE_GIT_AUTHOR_EMAIL="$(git log -1 --pretty=%ae)"
export CICD_RELEASE_GIT_COMMIT="$(git log -1 --pretty=%H)"
export CICD_RELEASE_GIT_SHORT_COMMIT="$(git log -1 --pretty=%h)"

echo "==> CICD_RELEASE_GIT_MESSAGE = $CICD_RELEASE_GIT_MESSAGE"
echo "==> CICD_RELEASE_GIT_AUTHOR = $CICD_RELEASE_GIT_AUTHOR"
echo "==> CICD_RELEASE_GIT_AUTHOR_EMAIL = $CICD_RELEASE_GIT_AUTHOR_EMAIL"
echo "==> CICD_RELEASE_GIT_COMMIT = $CICD_RELEASE_GIT_COMMIT"
echo "==> CICD_RELEASE_GIT_SHORT_COMMIT = $CICD_RELEASE_GIT_SHORT_COMMIT"
echo


echo "Verify gh command is on PATH"

if ! command -v gh &> /dev/null; then
    echo "==! Could not find gh command on PATH. EXITING"
    exit 1
fi

echo
echo "==> Promoting commits up to $CICD_RELEASE_GIT_SHORT_COMMIT to release candidate branch."
echo "==> Release candidate branch: $CICD_RELEASE_CANDIDATE_BRANCH"

echo "[command] git checkout -B $CICD_RELEASE_CANDIDATE_BRANCH $CICD_RELEASE_SOURCE_COMMIT"
git checkout -B $CICD_RELEASE_CANDIDATE_BRANCH $CICD_RELEASE_SOURCE_COMMIT
echo "[command] git push --set-upstream --force"
git push --set-upstream --force origin $CICD_RELEASE_CANDIDATE_BRANCH

CICD_RELEASE_DATE=$(date '+%Y-%m-%d')
CICD_RELEASE_PR_TITLE="Release $CICD_RELEASE_SOURCE_ENV_TYPE -> $CICD_RELEASE_TARGET_ENV_TYPE ($CICD_RELEASE_DATE)"

# Release tags chain each promotion to the next: this run's tag becomes the
# `previous_tag_name` for the next promotion's generated release notes.
CICD_RELEASE_TAG_NAMESPACE="cicd-release/$CICD_RELEASE_TARGET_BRANCH"
CICD_RELEASE_NEW_TAG="$CICD_RELEASE_TAG_NAMESPACE/$CICD_RELEASE_DATE-$CICD_RELEASE_GIT_SHORT_COMMIT"

CICD_RELEASE_PR_MESSAGE_FILE=$(mktemp)


###################################
# PR body rendering helpers
###################################

render_header() {
    cat <<EOF
# Release
## Release Summary
| Release Attribute | Value |
| --- | --- |
| Target Branch | $CICD_RELEASE_TARGET_BRANCH |
| Source Branch | $CICD_RELEASE_CANDIDATE_BRANCH ($CICD_RELEASE_GIT_SHORT_COMMIT) |
| Date          | $(date '+%Y-%m-%d %H:%M:%S') |

EOF
}

# Resolve the previous release tag: the most recent tag in our release-tag
# namespace that sits on the CURRENT SOURCE LINE (i.e. is an ancestor of the
# commit being promoted).
#
# We anchor on the source commit -- NOT the target branch -- so the chain
# survives every merge strategy. The promotion PR merges source -> target;
# squash/rebase merges rewrite commits on the TARGET side, but the source
# branch is never rewritten and only moves forward, so a tag created at a
# prior source tip stays an ancestor of the current source tip. `--merged`
# scopes selection to this source line, so tags from a different promotion lane
# (a different source branch sharing this target's tag namespace) are excluded.
#
# `grep -vxF` drops the tag this run is about to create, in case a same-day
# re-run of the same commit already pushed it. Prints the tag name, or empty
# when none exists (e.g. the first promotion into this target).
resolve_previous_release_tag() {
    git tag --list "$CICD_RELEASE_TAG_NAMESPACE/*" \
        --merged "$CICD_RELEASE_SOURCE_COMMIT" \
        --sort=-creatordate 2>/dev/null \
        | grep -vxF "$CICD_RELEASE_NEW_TAG" \
        | head -n1
}

# Fallback used whenever GitHub's generate-notes endpoint can't be reached
# (repo unconfigured, API error, etc.). Range is the previous release tag (or
# the target branch tip on the first release) up to the promoted commit.
render_release_notes_git_log() {
    local since="$1"
    local range
    if [[ -n "$since" ]]; then
        range="$since..$CICD_RELEASE_SOURCE_COMMIT"
    else
        range="origin/$CICD_RELEASE_TARGET_BRANCH..$CICD_RELEASE_SOURCE_COMMIT"
    fi
    git log "$range" --pretty=format:"- %s (%h) -- @%an" --no-merges 2>/dev/null \
        || echo "_(no commits found)_"
}

render_release_notes_body() {
    if [[ -z "$CICD_RELEASE_REPOSITORY" ]]; then
        echo "_Repository not configured; falling back to git log:_"
        echo
        render_release_notes_git_log ""
        return
    fi

    local previous_tag
    previous_tag=$(resolve_previous_release_tag)

    # GitHub's generate-notes builds categorized, PR-aware notes server-side in
    # a single call -- buckets and labels are controlled per-repo via
    # .github/release.yml. The change range is previous_tag -> source commit.
    # When there is no previous tag (first release into this branch), omitting
    # previous_tag_name lets GitHub auto-select; we instead fall back to git log
    # below for a deterministic full-history listing.
    if [[ -z "$previous_tag" ]]; then
        echo "_No previous release tag found; listing all commits up to the promoted revision:_"
        echo
        render_release_notes_git_log ""
        return
    fi

    local notes_json notes_body
    notes_json=$(gh api --method POST "repos/$CICD_RELEASE_REPOSITORY/releases/generate-notes" \
        -f "tag_name=$CICD_RELEASE_NEW_TAG" \
        -f "target_commitish=$CICD_RELEASE_SOURCE_COMMIT" \
        -f "previous_tag_name=$previous_tag" 2>/dev/null)

    if [[ -z "$notes_json" ]]; then
        echo "_GitHub release-notes API unavailable; falling back to git log:_"
        echo
        render_release_notes_git_log "$previous_tag"
        return
    fi

    notes_body=$(jq -r '.body // empty' <<< "$notes_json")
    if [[ -z "$notes_body" ]]; then
        echo "_No changes since previous release ($previous_tag)._"
        return
    fi

    echo "_Changes since $previous_tag._"
    echo
    echo "$notes_body"
}

render_llm_summary() {
    [[ "$CICD_RELEASE_LLM_SUMMARY" == "true" ]] || return 0

    if ! command -v aws &> /dev/null; then
        echo "==! aws CLI not found; skipping LLM summary." >&2
        return 0
    fi

    local notes_body="$1"
    [[ -n "$notes_body" ]] || return 0

    local model_id="${CICD_RELEASE_LLM_MODEL_ID}"

    local request_file response_file
    request_file=$(mktemp)
    response_file=$(mktemp)

    jq -n --arg notes "$notes_body" '{
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: 500,
        messages: [{
            role: "user",
            content: ("Summarize this software release in 1-2 paragraphs for a deployment PR description. Focus on themes and user-facing impact. Do not restate the bullet list verbatim. Release contents:\n\n" + $notes)
        }]
    }' > "$request_file"
    if aws bedrock-runtime invoke-model \
        --model-id "$model_id" \
        --body "fileb://$request_file" \
        --content-type application/json \
        "$response_file" &> /dev/null; then
        local summary
        summary=$(jq -r '.content[0].text // empty' "$response_file" 2>/dev/null)
        if [[ -n "$summary" ]]; then
            echo "## Summary"
            echo
            echo "$summary"
            echo
        fi
    else
        echo "==! Bedrock invoke failed; skipping LLM summary." >&2
    fi

    rm -f "$request_file" "$response_file"
}

render_checklist() {
    [[ -n "$CICD_RELEASE_CHECKLIST" ]] || return 0

    echo "## Checklist"
    echo
    while IFS= read -r item; do
        [[ -n "$item" ]] && echo "- [ ] $item"
    done <<< "$CICD_RELEASE_CHECKLIST"
    echo
}

render_extra_sections() {
    [[ -n "$CICD_RELEASE_EXTRA_SECTIONS_JSON" ]] || return 0
    [[ "$CICD_RELEASE_EXTRA_SECTIONS_JSON" == "{}" ]] && return 0

    local headings
    headings=$(jq -r 'keys[]' <<< "$CICD_RELEASE_EXTRA_SECTIONS_JSON" 2>/dev/null) || return 0

    while IFS= read -r heading; do
        [[ -n "$heading" ]] || continue
        local body
        body=$(jq -r --arg h "$heading" '.[$h]' <<< "$CICD_RELEASE_EXTRA_SECTIONS_JSON")
        echo "## $heading"
        echo
        echo "$body"
        echo
    done <<< "$headings"
}

###################################
# Compose PR body
###################################

RELEASE_NOTES_BODY=$(render_release_notes_body)

{
    render_header
    render_llm_summary "$RELEASE_NOTES_BODY"
    echo "## Release Notes"
    echo
    echo "This release includes changes up to $CICD_RELEASE_GIT_SHORT_COMMIT."
    echo
    echo "$RELEASE_NOTES_BODY"
    echo
    render_checklist
    render_extra_sections
} > "$CICD_RELEASE_PR_MESSAGE_FILE"


echo "==> Checking for open Pull Requests..."
EXISTING_PR_NUMBER=$(gh pr list -B $CICD_RELEASE_TARGET_BRANCH -L 1 | cut -f1)

if [[ ! -z $EXISTING_PR_NUMBER ]]; then
    echo "==> Pull Request already exists ($EXISTING_PR_NUMBER). Updating..."

    # Append the previous PR body as "Previous Revision", capped at ONE level of
    # history. Earlier this appended the entire prior body -- which itself
    # already contained a Previous Revision section -- so every update re-nested
    # all prior history and the body grew ~quadratically toward GitHub's 65,536
    # char PR-body limit. The HTML-comment sentinel marks where this run's own
    # content ends and inherited history begins; we keep only the previous
    # revision's content up to ITS sentinel, dropping everything it inherited.
    PREVIOUS_BODY=$(gh pr view "$EXISTING_PR_NUMBER" --json body | jq -r '.body')
    PREVIOUS_BODY=$(awk '/<!-- cicd-release:history-below -->/{exit} {print}' <<< "$PREVIOUS_BODY")
    {
        echo ""
        echo "<!-- cicd-release:history-below -->"
        echo "---"
        echo "# Previous Revision"
        echo "---"
        echo ""
        printf '%s\n' "$PREVIOUS_BODY"
    } >> "$CICD_RELEASE_PR_MESSAGE_FILE"

    gh pr edit $EXISTING_PR_NUMBER \
        --title "$CICD_RELEASE_PR_TITLE" \
        --body-file $CICD_RELEASE_PR_MESSAGE_FILE

else

    echo "==> Creating new Pull Request"

    # Only pass --reviewer when reviewers are configured; gh errors on an empty value.
    reviewer_args=()
    if [[ -n "$CICD_RELEASE_REVIEWER" ]]; then
        reviewer_args=(--reviewer "$CICD_RELEASE_REVIEWER")
    fi

    gh pr create \
        --base $CICD_RELEASE_TARGET_BRANCH \
        --title "$CICD_RELEASE_PR_TITLE" \
        --body-file "$CICD_RELEASE_PR_MESSAGE_FILE" \
        "${reviewer_args[@]}"
fi


###################################
# Tag the promoted revision
###################################
# This tag becomes the `previous_tag_name` for the NEXT promotion's generated
# release notes, chaining each release to the last. It is created at the source
# (promoted) commit, on the source line. resolve_previous_release_tag() selects
# the next range start from this same source line (`git tag --merged
# $SOURCE_COMMIT`), so the chain is independent of how the promotion PR merges
# into the target -- merge commit, squash, and rebase all work, because the
# source branch is never rewritten.
echo
echo "==> Tagging promoted revision: $CICD_RELEASE_NEW_TAG -> $CICD_RELEASE_GIT_SHORT_COMMIT"
git tag -f "$CICD_RELEASE_NEW_TAG" "$CICD_RELEASE_SOURCE_COMMIT"
if git push -f origin "refs/tags/$CICD_RELEASE_NEW_TAG"; then
    echo "==> Pushed release tag $CICD_RELEASE_NEW_TAG"
else
    echo "==! Failed to push release tag $CICD_RELEASE_NEW_TAG; next release's notes range may be wider than intended." >&2
fi
