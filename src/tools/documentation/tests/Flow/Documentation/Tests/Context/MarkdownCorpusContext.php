<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Context;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

use function sort;

final readonly class MarkdownCorpusContext
{
    /**
     * @param list<string> $roots
     *
     * @return list<string>
     */
    public function filesIn(array $roots): array
    {
        $files = [];

        foreach ($roots as $root) {
            /** @var iterable<SplFileInfo> $found */
            $found = new RecursiveIteratorIterator(new RecursiveDirectoryIterator(
                $root,
                FilesystemIterator::SKIP_DOTS,
            ));

            foreach ($found as $file) {
                if ($file->getExtension() === 'md') {
                    $files[] = $file->getPathname();
                }
            }
        }

        sort($files);

        return $files;
    }
}
