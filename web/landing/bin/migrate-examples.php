#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * Migration script to copy examples from /examples/topics/ to /web/landing/content/examples/topics/.
 *
 * Files copied:
 * - code.php
 * - description.md
 *
 * Files generated:
 * - _meta.yaml (from priority.txt and hidden.txt)
 *
 * Files skipped:
 * - composer.json
 * - composer.lock
 * - flow_php_example.zip
 * - output.*
 * - input/
 */
$sourceRoot = dirname(__DIR__, 3) . '/examples/topics';
$targetRoot = dirname(__DIR__) . '/content/examples/topics';

if (!is_dir($sourceRoot)) {
    print "Error: Source directory does not exist: {$sourceRoot}\n";

    exit(1);
}

print "Migrating examples from:\n  {$sourceRoot}\nto:\n  {$targetRoot}\n\n";

$migratedCount = 0;

function ensureDirectory(string $path) : void
{
    if (!is_dir($path)) {
        mkdir($path, 0755, true);
        print "  Created directory: {$path}\n";
    }
}

function getMetaYaml(string $sourcePath) : string
{
    $priorityPath = $sourcePath . '/priority.txt';
    $hiddenPath = $sourcePath . '/hidden.txt';

    $priority = file_exists($priorityPath) ? (int) trim(file_get_contents($priorityPath)) : 99;
    $hidden = file_exists($hiddenPath);

    return "priority: {$priority}\nhidden: " . ($hidden ? 'true' : 'false') . "\n";
}

function migrateExample(string $sourcePath, string $targetPath) : bool
{
    ensureDirectory($targetPath);

    $migrated = false;

    // Copy code.php
    $codeSource = $sourcePath . '/code.php';

    if (file_exists($codeSource)) {
        $codeTarget = $targetPath . '/code.php';
        copy($codeSource, $codeTarget);
        print "    Copied: code.php\n";
        $migrated = true;
    }

    // Copy description.md
    $descSource = $sourcePath . '/description.md';

    if (file_exists($descSource)) {
        $descTarget = $targetPath . '/description.md';
        copy($descSource, $descTarget);
        print "    Copied: description.md\n";
    }

    // Generate _meta.yaml
    $metaTarget = $targetPath . '/_meta.yaml';
    file_put_contents($metaTarget, getMetaYaml($sourcePath));
    print "    Generated: _meta.yaml\n";

    return $migrated;
}

// Iterate through topics
$topics = array_values(array_diff(scandir($sourceRoot), ['..', '.', '.gitignore']));

foreach ($topics as $topic) {
    $topicSourcePath = $sourceRoot . '/' . $topic;

    if (!is_dir($topicSourcePath)) {
        continue;
    }

    print "\nTopic: {$topic}\n";
    $topicTargetPath = $targetRoot . '/' . $topic;
    ensureDirectory($topicTargetPath);

    // Generate topic-level _meta.yaml
    $metaTarget = $topicTargetPath . '/_meta.yaml';
    file_put_contents($metaTarget, getMetaYaml($topicSourcePath));
    print "  Generated: _meta.yaml\n";

    // Iterate through examples
    $examples = array_values(array_diff(scandir($topicSourcePath), ['..', '.', '.gitignore', 'priority.txt', 'hidden.txt']));

    foreach ($examples as $example) {
        $exampleSourcePath = $topicSourcePath . '/' . $example;

        if (!is_dir($exampleSourcePath)) {
            continue;
        }

        print "  Example: {$example}\n";
        $exampleTargetPath = $topicTargetPath . '/' . $example;

        // Check if example has code.php (2-level structure) or has subdirectories (3-level structure)
        if (file_exists($exampleSourcePath . '/code.php')) {
            // 2-level structure: example has code.php directly
            if (migrateExample($exampleSourcePath, $exampleTargetPath)) {
                $migratedCount++;
            }
        } else {
            // 3-level structure: example has options
            ensureDirectory($exampleTargetPath);

            // Generate example-level _meta.yaml
            $metaTarget = $exampleTargetPath . '/_meta.yaml';
            file_put_contents($metaTarget, getMetaYaml($exampleSourcePath));
            print "    Generated: _meta.yaml\n";

            $options = array_values(array_diff(scandir($exampleSourcePath), ['..', '.', '.gitignore', 'priority.txt', 'hidden.txt']));

            foreach ($options as $option) {
                $optionSourcePath = $exampleSourcePath . '/' . $option;

                if (!is_dir($optionSourcePath)) {
                    continue;
                }

                if (!file_exists($optionSourcePath . '/code.php')) {
                    continue;
                }

                print "    Option: {$option}\n";
                $optionTargetPath = $exampleTargetPath . '/' . $option;

                if (migrateExample($optionSourcePath, $optionTargetPath)) {
                    $migratedCount++;
                }
            }
        }
    }
}

print "\n\nMigration complete! Migrated {$migratedCount} examples.\n";
