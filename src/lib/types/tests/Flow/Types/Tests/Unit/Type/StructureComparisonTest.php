<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type\Comparator;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\StructureComparison;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class StructureComparisonTest extends TestCase
{
    public function test_identical_is_positional(): void
    {
        $comparison = new StructureComparison();
        $comparator = new Comparator();

        static::assertTrue($comparison->identical(
            new StructureType([new StructureElement('a', type_integer()), new StructureElement('b', type_string())]),
            new StructureType([new StructureElement('a', type_integer()), new StructureElement('b', type_string())]),
            $comparator,
        ));
        static::assertFalse($comparison->identical(
            new StructureType([new StructureElement('a', type_integer()), new StructureElement('b', type_string())]),
            new StructureType([new StructureElement('b', type_string()), new StructureElement('a', type_integer())]),
            $comparator,
        ));
    }

    public function test_identical_includes_the_optional_flag(): void
    {
        static::assertFalse((new StructureComparison())->identical(
            new StructureType([new StructureElement('a', type_integer())]),
            new StructureType([new StructureElement('a', type_integer(), optional: true)]),
            new Comparator(),
        ));
    }

    public function test_identical_includes_allows_extra(): void
    {
        static::assertFalse((new StructureComparison())->identical(
            new StructureType([new StructureElement('a', type_integer())], true),
            new StructureType([new StructureElement('a', type_integer())]),
            new Comparator(),
        ));
    }
}
