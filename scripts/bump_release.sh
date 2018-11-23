#!/usr/bin/env bash

function write_at_begening() {
  { echo "$1"; cat $2; } >$2.new
  mv $2{.new,}
}

function write_release_note_between(){
  from=$1
  to=$2
  write_at_begening "$(git log ${from}..${to} --pretty=tformat:'- %s %h (%an)' | grep -v "Merge pull request #" | grep -v  "\[RELEASE\] ")" release-notes.md
  write_at_begening "### ${to} - $(git log -1 --format=%ad --date=short ${to})" release-notes.md
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
REMOTE_TAG_REFS=$(git ls-remote --refs --tags https://github.com/criteo/garmadon | cut -f1)
from=$(git log --pretty=format:%H|tail -1) # First commit
for ref in $REMOTE_TAG_REFS
do
  #Tag branches
  branches=$(git branch --contains ${ref})
  #Consider only master branch
  if [[ $branches =~ 'master' ]]
  then
    #Get the first release tag pointing at this commit
    tag=$(git tag --points-at ${ref} | grep v | head -n 1)

    #Consider only release tag
    if [ ! -z "$tag" ]
    then
      write_release_note_between $from $tag 
      from=${tag}
    fi
  fi  
done

#There is only the tag we created for this release left
write_release_note_between ${from} v${NEW_GARMADON_RELEASE}

write_at_begening "----------------------" release-notes.md
write_at_begening "Garmadon release notes" release-notes.md

#  Commit release-notes update
git add release-notes.md
git commit -m "[RELEASE] Update release note ${NEW_GARMADON_RELEASE}"

popd
