<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use RuntimeException;
use SplFileInfo;

use function hash;
use function hash_file;
use function is_file;
use function ksort;
use function ltrim;
use function strlen;
use function substr;

final readonly class SourceTreeDigest
{
    public function __construct(
        private string $relativeTree,
    ) {}

    public function value(): string
    {
        $root = Paths::projectRoot();
        $absolute = $root . '/' . $this->relativeTree;
        $pairs = [];

        if (is_file($absolute)) {
            $pairs[$this->relativeTree] = self::digestOf($absolute);
        } else {
            /** @var SplFileInfo $file */
            foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(
                $absolute,
                FilesystemIterator::SKIP_DOTS,
            )) as $file) {
                if ($file->isFile() && $file->getExtension() === 'php') {
                    $pairs[ltrim(substr($file->getPathname(), strlen($root)), '/')] = self::digestOf(
                        $file->getPathname(),
                    );
                }
            }
        }

        ksort($pairs);

        $buffer = '';

        foreach ($pairs as $relativePath => $fileHash) {
            $buffer .= $relativePath . ':' . $fileHash . "\n";
        }

        return hash('xxh128', $buffer);
    }

    /**
     * An unreadable file would otherwise fold `false` into the buffer and silently produce a
     * fingerprint that no longer identifies the code it names.
     */
    public static function digestOf(string $file): string
    {
        $digest = hash_file('xxh128', $file);

        if ($digest === false) {
            throw new RuntimeException('Cannot digest source file: ' . $file);
        }

        return $digest;
    }
}
