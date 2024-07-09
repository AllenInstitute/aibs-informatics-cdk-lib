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


cat <<EOF > $CICD_RELEASE_PR_MESSAGE_FILE
# Release
## Release Summary
| Release Attribute | Value |
| --- | --- |
| Target Branch | $CICD_RELEASE_TARGET_BRANCH |
| Source Branch | $CICD_RELEASE_CANDIDATE_BRANCH ($CICD_RELEASE_GIT_SHORT_COMMIT) |
| Date          | $(date '+%Y-%m-%d %H:%M:%S') |

## Release Notes

This release includes changes up to $CICD_RELEASE_GIT_SHORT_COMMIT. This includes the following:
- (fill me please)
- (fill me please)
- (fill me please)

## Checklist
- [ ] All of GCS works impeccably

EOF


echo "==> Checking for open Pull Requests..."
EXISTING_PR_NUMBER=$(gh pr list -B $CICD_RELEASE_TARGET_BRANCH -L 1 | cut -f1)

if [[ ! -z $EXISTING_PR_NUMBER ]]; then
    echo "==> Pull Request already exists ($EXISTING_PR_NUMBER). Updating..."

    # Update the PR message
    echo "" | cat >> $CICD_RELEASE_PR_MESSAGE_FILE 
    echo "---" | cat >> $CICD_RELEASE_PR_MESSAGE_FILE 
    echo "# Previous Revisions" | cat >> $CICD_RELEASE_PR_MESSAGE_FILE
    echo "---" | cat >> $CICD_RELEASE_PR_MESSAGE_FILE
    echo "" | cat >> $CICD_RELEASE_PR_MESSAGE_FILE
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