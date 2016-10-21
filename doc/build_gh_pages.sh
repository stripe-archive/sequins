#!/bin/sh

set -eu
cd $(dirname $0)/..
VERSION=$(git rev-parse HEAD)

TMP_OUT=$(mktemp -d)
echo "Building gh-pages in $TMP_OUT"

gitbook install doc/manual
gitbook build doc/manual $TMP_OUT/manual
cp doc/landing.html $TMP_OUT/index.html
cp doc/sequins.svg doc/sequins_small.png doc/font-awesome.min.css doc/flexblocks-responsive.css $TMP_OUT/
echo sequins.io > $TMP_OUT/CNAME

cd $TMP_OUT
git init
git remote add origin git@github.com:stripe/sequins
git add .
git commit -m "gh-pages generated from $VERSION at $(date -u)"
git push --force origin master:gh-pages

rm -rf $TMP_OUT
echo "Done! https://stripe.github.io/sequins"
