<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use PHPUnit\Framework\Assert;
use ReflectionClass;

use function method_exists;

final class MetadataColumnsContract
{
    /**
     * @param class-string<Extractor> $class
     */
    public static function declares(string $class): void
    {
        Assert::assertTrue((new ReflectionClass($class))->implementsInterface(MetadataColumnsExtractor::class), $class);
        Assert::assertTrue(method_exists($class, 'withMetadataColumns'), $class);
    }

    /**
     * A source that has nothing to describe must not carry the setter at all - a no-op withMetadataColumns() reads as
     * "supported, and it did nothing".
     *
     * @param class-string<Extractor> $class
     */
    public static function doesNotCarryTheSetter(string $class): void
    {
        Assert::assertFalse(method_exists($class, 'withMetadataColumns'), $class);
    }
}
