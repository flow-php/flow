<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Row\UnresolvedReference;
use ReflectionClass;

use function array_filter;
use function array_unique;
use function array_values;
use function class_exists;
use function sort;

/**
 * Every ScalarFunction implementation shipped in src/, discovered by scanning rather than by an
 * allow-list, so a new function cannot be added without the guards seeing it.
 */
final class ScalarFunctionClasses
{
    /**
     * Every class except UnresolvedReference, which resolves into ResolvedReference instead of
     * declaring anything itself.
     *
     * @return list<class-string<ScalarFunction>>
     */
    public static function declaring(): array
    {
        return array_values(array_filter(
            self::all(),
            static fn(string $class): bool => $class !== UnresolvedReference::class,
        ));
    }

    /**
     * @return list<class-string<ScalarFunction>>
     */
    public static function all(): array
    {
        return self::implementing(ScalarFunction::class);
    }

    /**
     * Every concrete AggregatingFunction and/or WindowFunction implementation shipped in src/ - the
     * producers that declare a column without being scalar functions.
     *
     * @return list<class-string<AggregatingFunction|WindowFunction>>
     */
    public static function producing(): array
    {
        $found = [...self::implementing(AggregatingFunction::class), ...self::implementing(WindowFunction::class)];
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
    public static function implementing(string $interface): array
    {
        $found = [];

        foreach (ExtractorClasses::sourceFiles() as $file) {
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
