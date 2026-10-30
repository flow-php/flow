<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\WindowFunction;
use ReflectionClass;

use function array_filter;
use function array_unique;
use function array_values;
use function class_exists;
use function sort;

/**
 * Every ScalarFunction implementation one package ships, discovered by scanning rather than by an allow-list, so a new
 * function cannot be added without the guards seeing it.
 */
final class ScalarFunctionClasses
{
    /**
     * Every class except UnresolvedReference, which resolves into ResolvedReference instead of
     * declaring anything itself.
     *
     * @return list<class-string<ScalarFunction>>
     */
    public static function declaring(string $sourceRoot): array
    {
        return array_values(array_filter(
            self::all($sourceRoot),
            static fn(string $class): bool => $class !== UnresolvedReference::class,
        ));
    }

    /**
     * @return list<class-string<ScalarFunction>>
     */
    public static function all(string $sourceRoot): array
    {
        return self::implementing(ScalarFunction::class, $sourceRoot);
    }

    /**
     * Every concrete AggregatingFunction and/or WindowFunction implementation one package ships - the producers that
     * declare a column without being scalar functions.
     *
     * @return list<class-string<AggregatingFunction|WindowFunction>>
     */
    public static function producing(string $sourceRoot): array
    {
        $found = [
            ...self::implementing(AggregatingFunction::class, $sourceRoot),
            ...self::implementing(WindowFunction::class, $sourceRoot),
        ];
        sort($found);

        return array_values(array_unique($found));
    }

    /**
     * @template T of object
     *
     * @param class-string<T> $interface
     *
     * @return list<class-string<T>>
     */
    public static function implementing(string $interface, string $sourceRoot): array
    {
        $found = [];

        foreach (ExtractorClasses::sourceFiles($sourceRoot) as $file) {
            $class = ExtractorClasses::classIn($file);

            if ($class === null || !class_exists($class)) {
                continue;
            }

            $reflection = new ReflectionClass($class);

            if (!$reflection->implementsInterface($interface) || $reflection->isAbstract()) {
                continue;
            }

            $found[] = $class;
        }

        sort($found);

        return $found;
    }
}
