<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use FilesystemIterator;
use Flow\ETL\Extractor;
use Generator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use ReflectionClass;
use SplFileInfo;

use function class_exists;
use function dirname;
use function preg_match;
use function sort;

/**
 * Every Extractor implementation shipped in src/, discovered by scanning rather than by an
 * allow-list, so a new source cannot be added without the guards seeing it.
 */
final class ExtractorClasses
{
    /**
     * @return list<class-string<Extractor>>
     */
    public static function all(): array
    {
        $found = [];

        foreach (self::sourceFiles() as $file) {
            $class = self::classIn($file);

            if ($class === null || !class_exists($class)) {
                continue;
            }

            $reflection = new ReflectionClass($class);

            if (!$reflection->implementsInterface(Extractor::class) || $reflection->isAbstract()) {
                continue;
            }

            /** @var class-string<Extractor> $class */
            $found[] = $class;
        }

        sort($found);

        return $found;
    }

    /**
     * @return \Generator<int, string>
     */
    public static function sourceFiles(): Generator
    {
        $root = dirname(__DIR__, 8);

        foreach (['core', 'adapter', 'bridge', 'lib', 'cli'] as $group) {
            $directory = $root . '/src/' . $group;

            if (!is_dir($directory)) {
                continue;
            }

            $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator(
                $directory,
                FilesystemIterator::SKIP_DOTS,
            ));

            foreach ($iterator as $file) {
                if (!$file instanceof SplFileInfo) {
                    continue;
                }

                $path = $file->getPathname();

                if (!str_ends_with($path, '.php')) {
                    continue;
                }

                if (str_contains($path, '/tests/') || str_contains($path, '/vendor/')) {
                    continue;
                }

                yield $path;
            }
        }
    }

    public static function classIn(string $file): ?string
    {
        $source = file_get_contents($file);

        if ($source === false) {
            return null;
        }

        $namespace = [];
        $class = [];

        if (!preg_match('/^namespace\s+([^;]+);/m', $source, $namespace)) {
            return null;
        }

        if (!preg_match('/^(?:final\s+)?(?:readonly\s+)?class\s+(\w+)/m', $source, $class)) {
            return null;
        }

        return $namespace[1] . '\\' . $class[1];
    }
}
