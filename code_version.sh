#!/bin/bash

# THIS IS APPLE ONLY. DO NOT RELEASE

if [[ -n "$(awk -F'[ \t]*=[ \t]*' '/^version/ { print $2; exit }' gradle.properties 2>/dev/null)" ]]; then
    version=$(awk -F'[ \t]*=[ \t]*' '/^version/ { print $2; exit }' gradle.properties)
    from_statement="from 'gradle.properties'"
elif [[ -n "$(awk -F'[<>]' 'NR>1 && /^[ \t]*<[^!]/ && /<version/ { print $3; exit }' pom.xml 2>/dev/null)" ]]; then
    version=$(awk -F'[<>]' 'NR>1 && /^[ \t]*<[^!]/ && /<version/ { print $3; exit }' pom.xml)
    from_statement="from 'pom.xml'"
elif [[ -n "$(awk '/defproject/ { gsub("\"|-SNAPSHOT", ""); print $3 }' project.clj 2>/dev/null)" ]]; then
    version=$(awk '/defproject/ { gsub("\"|-SNAPSHOT", ""); print $3 }' project.clj)
    from_statement="from 'project.clj'"
elif [[ -f version ]]; then
    version=$(cat version)
    from_statement="from toplevel 'version' file"
else
    version=$(git rev-parse --short=12 HEAD)
    from_statement="from git hash (fallback method)"
fi

# Remove quotes from the version if present
version=${version//\"/}
version=${version//\'/}

echo "Parsed version $from_statement: '$version'" 1>&2

versionSuffix=${1}
if [[ -z ${versionSuffix} ]]; then
    echo "$version"
else
    echo "$version-${versionSuffix}"
fi
