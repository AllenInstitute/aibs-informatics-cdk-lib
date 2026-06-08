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

render_release_notes_body() {
    local base="$CICD_RELEASE_TARGET_BRANCH"
    local head="$CICD_RELEASE_SOURCE_COMMIT"

    if [[ -z "$CICD_RELEASE_REPOSITORY" ]]; then
        echo "_Repository not configured; falling back to git log:_"
        echo
        git log "origin/$base..$head" --pretty=format:"- %s (%h) -- @%an" --no-merges 2>/dev/null \
            || echo "_(no commits found)_"
        return
    fi

    local compare_json
    compare_json=$(gh api "repos/$CICD_RELEASE_REPOSITORY/compare/$base...$head" 2>/dev/null)

    if [[ -z "$compare_json" ]]; then
        echo "_GitHub compare API unavailable; falling back to git log:_"
        echo
        git log "origin/$base..$head" --pretty=format:"- %s (%h) -- @%an" --no-merges 2>/dev/null \
            || echo "_(no commits found)_"
        return
    fi

    local commit_count
    commit_count=$(jq '.commits | length' <<< "$compare_json")

    if [[ "$commit_count" == "0" ]]; then
        echo "_No new commits in this release._"
        return
    fi

    if [[ "$commit_count" -ge 250 ]]; then
        echo "_Release exceeds GitHub compare API limit (250 commits); falling back to git log:_"
        echo
        git log "origin/$base..$head" --pretty=format:"- %s (%h) -- @%an" --no-merges
        return
    fi

    local breaking="" features="" fixes="" other="" direct=""
    local seen_prs=" "
    local sha

    while IFS= read -r sha; do
        local pulls_json
        pulls_json=$(gh api "repos/$CICD_RELEASE_REPOSITORY/commits/$sha/pulls" 2>/dev/null)

        if [[ -z "$pulls_json" || "$(jq 'length' <<< "$pulls_json" 2>/dev/null)" == "0" ]]; then
            local subj short_sha
            subj=$(jq -r --arg s "$sha" '.commits[] | select(.sha == $s) | .commit.message' <<< "$compare_json" | head -n1)
            short_sha=${sha:0:7}
            direct+="- ${subj:-$sha} ($short_sha)"$'\n'
            continue
        fi

        local pr_number pr_title pr_user pr_labels entry
        pr_number=$(jq -r '.[0].number' <<< "$pulls_json")

        if [[ "$seen_prs" == *" $pr_number "* ]]; then
            continue
        fi
        seen_prs+="$pr_number "

        pr_title=$(jq -r '.[0].title' <<< "$pulls_json")
        pr_user=$(jq -r '.[0].user.login' <<< "$pulls_json")
        pr_labels=$(jq -r '.[0].labels[]?.name // empty' <<< "$pulls_json" | tr '\n' ' ')

        entry="- #$pr_number $pr_title (@$pr_user)"$'\n'

        if echo "$pr_labels" | grep -qiE '(^| )(breaking|breaking-change)( |$)'; then
            breaking+="$entry"
        elif echo "$pr_labels" | grep -qiE '(^| )(feat|feature|enhancement)( |$)'; then
            features+="$entry"
        elif echo "$pr_labels" | grep -qiE '(^| )(fix|bug|bugfix)( |$)'; then
            fixes+="$entry"
        else
            other+="$entry"
        fi
    done < <(jq -r '.commits[].sha' <<< "$compare_json")

    if [[ -n "$breaking" ]]; then
        echo "### Breaking Changes"
        echo
        printf '%s' "$breaking"
        echo
    fi
    if [[ -n "$features" ]]; then
        echo "### Features"
        echo
        printf '%s' "$features"
        echo
    fi
    if [[ -n "$fixes" ]]; then
        echo "### Fixes"
        echo
        printf '%s' "$fixes"
        echo
    fi
    if [[ -n "$other" ]]; then
        echo "### Other Changes"
        echo
        printf '%s' "$other"
        echo
    fi
    if [[ -n "$direct" ]]; then
        echo "### Direct Commits"
        echo
        printf '%s' "$direct"
        echo
    fi
}

render_llm_summary() {
    [[ "$CICD_RELEASE_LLM_SUMMARY" == "true" ]] || return 0

    if ! command -v aws &> /dev/null; then
        echo "==! aws CLI not found; skipping LLM summary." >&2
        return 0
    fi

    local notes_body="$1"
    [[ -n "$notes_body" ]] || return 0

    local model_id="${CICD_RELEASE_LLM_MODEL_ID:-anthropic.claude-haiku-4-5}"

    local request_file response_file
    request_file=$(mktemp)
    response_file=$(mktemp)

    jq -n --arg notes "$notes_body" '{
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: 500,
        messages: [{
            role: "user",
            content: ("Summarize this software release in 2-3 sentences for a deployment PR description. Focus on themes and user-facing impact. Do not restate the bullet list verbatim. Release contents:\n\n" + $notes)
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

    # Update the PR message
    echo "" >> $CICD_RELEASE_PR_MESSAGE_FILE
    echo "---" >> $CICD_RELEASE_PR_MESSAGE_FILE
    echo "# Previous Revisions" >> $CICD_RELEASE_PR_MESSAGE_FILE
    echo "---" >> $CICD_RELEASE_PR_MESSAGE_FILE
    echo "" >> $CICD_RELEASE_PR_MESSAGE_FILE
    gh pr view --json body | jq -r '.body' >> $CICD_RELEASE_PR_MESSAGE_FILE

    gh pr edit $EXISTING_PR_NUMBER \
        --title "$CICD_RELEASE_PR_TITLE" \
        --body-file $CICD_RELEASE_PR_MESSAGE_FILE

else

    echo "==> Creating new Pull Request"

    gh pr create \
        --base $CICD_RELEASE_TARGET_BRANCH \
        --title "$CICD_RELEASE_PR_TITLE" \
        --body-file "$CICD_RELEASE_PR_MESSAGE_FILE" \
        --reviewer "$CICD_RELEASE_REVIEWER"
fi
