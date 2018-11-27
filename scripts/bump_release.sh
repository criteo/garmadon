#!/usr/bin/env bash

function write_at_begening() {
  { echo "$1"; cat $2; } >$2.new
  mv $2{.new,}
}

if [ "$#" -ne 1 ]; then
    echo "Usage:"
    echo "  $(basename "$0") <new release>"
    exit 1
fi

set -o pipefail
set -e
set -x

NEW_GARMADON_RELEASE=$1
BASEDIR=$(dirname "$0")/..

pushd ${BASEDIR}

# Create/Switch to release branch locally
git checkout release_$NEW_GARMADON_RELEASE || git checkout -b release_$NEW_GARMADON_RELEASE

# Bump release
mvn versions:set -DnewVersion=${NEW_GARMADON_RELEASE}
find . -name pom.xml.versionsBackup -delete
if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' "s#<garmadon.version>.*</garmadon.version>#<garmadon.version>${NEW_GARMADON_RELEASE}</garmadon.version>#g" pom.xml
else
  sed -i "s#<garmadon.version>.*</garmadon.version>#<garmadon.version>${NEW_GARMADON_RELEASE}</garmadon.version>#g" pom.xml
fi

# Commit release
find . -name pom.xml | xargs git add
git commit -m "[RELEASE] Create release ${NEW_GARMADON_RELEASE}"

#  Create release tag
git tag v${NEW_GARMADON_RELEASE}

#  Create release-notes
> release-notes.md
from=$(git log --pretty=format:%H|tail -1) # First commit
for tag in $(git tag)
do
  write_at_begening "$(git log ${from}..${tag} --pretty=tformat:'- %s %h (%an)' | grep -v "Merge pull request #" | grep -v  "\[RELEASE\] ")" release-notes.md
  write_at_begening "### ${tag} - $(git log -1 --format=%ad --date=short ${tag})" release-notes.md
  from=${tag}
done

write_at_begening "----------------------" release-notes.md
write_at_begening "Garmadon release notes" release-notes.md

#  Commit release-notes update
git add release-notes.md
git commit -m "[RELEASE] Update release note ${NEW_GARMADON_RELEASE}"

popd
