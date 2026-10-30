<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use FilesystemIterator;
use Flow\ETL\IO\Extractor;
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
 * Every Extractor implementation one package ships, discovered by scanning rather than by an allow-list, so a new
 * source cannot be added without the guards seeing it.
 */
final class ExtractorClasses
{
    /**
     * @return list<class-string<Extractor>>
     */
    public static function all(string $sourceRoot): array
    {
        $found = [];

        foreach (self::sourceFiles($sourceRoot) as $file) {
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
     * The src/ directory of one monorepo package, as "core/etl" or "adapter/etl-adapter-csv" names it.
     */
    public static function root(string $package): string
    {
        return dirname(__DIR__, 8) . '/src/' . $package . '/src';
    }

    /**
     * @return \Generator<int, string>
     */
    public static function sourceFiles(string $sourceRoot): Generator
    {
        $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator(
            $sourceRoot,
            FilesystemIterator::SKIP_DOTS,
        ));

        foreach ($iterator as $file) {
            if (!$file instanceof SplFileInfo) {
                continue;
            }

            $path = $file->getPathname();

            if (str_ends_with($path, '.php')) {
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
