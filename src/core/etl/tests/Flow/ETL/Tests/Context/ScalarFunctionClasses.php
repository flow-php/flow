<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row\UnresolvedReference;
use ReflectionClass;

use function array_filter;
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
        $found = [];

        foreach (ExtractorClasses::sourceFiles() as $file) {
            $class = ExtractorClasses::classIn($file);

            if ($class === null || !class_exists($class)) {
                continue;
            }

            $reflection = new ReflectionClass($class);

            if (!$reflection->implementsInterface(ScalarFunction::class) || $reflection->isAbstract()) {
                continue;
            }

            /** @var class-string<ScalarFunction> $class */
            $found[] = $class;
        }

        sort($found);

        return $found;
    }
}
