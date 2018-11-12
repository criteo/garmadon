#!/usr/bin/env bash

# Deploy on maven central
mvn deploy -P release --settings .travis/settings.xml -DskipTests