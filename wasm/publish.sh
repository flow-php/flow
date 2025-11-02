#!/bin/bash

# Simple deployment script for Flow-in-Browser
# Publishes the built files to GitHub Pages

set -e

echo "Publishing Flow-in-Browser to GitHub Pages..."

# Create a temporary directory for gh-pages
mkdir -p gh-pages
cp php.js php.wasm flow-*.phar index.html gh-pages/
cp -r assets gh-pages/

cd gh-pages
git init
git add .
git commit -m "Update Flow-in-Browser"

# Update this to your repository
git remote add origin https://github.com/yourusername/flow-in-browser.git
git push origin master:gh-pages -f

cd ..
rm -rf gh-pages

echo "Published successfully!"
