<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Schema\Definition\ElementCompatibility;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ElementCompatibilityTest extends FlowTestCase
{
    public static function provideCases(): Generator
    {
        yield 'identical types' => [type_string(), type_string(), true];

        yield 'different types' => [type_string(), type_integer(), false];

        yield 'declared optional, given the same base type' => [
            type_optional(type_string()),
            type_string(),
            true,
        ];

        yield 'declared optional, given the same optional type' => [
            type_optional(type_string()),
            type_optional(type_string()),
            true,
        ];

        yield 'declared optional, given null' => [
            type_optional(type_string()),
            type_null(),
            true,
        ];

        yield 'declared required, given null' => [
            type_string(),
            type_null(),
            false,
        ];

        yield 'declared required, given optional' => [
            type_string(),
            type_optional(type_string()),
            false,
        ];

        yield 'declared optional, given a different base type' => [
            type_optional(type_string()),
            type_boolean(),
            false,
        ];

        yield 'nested structure with an optional element' => [
            type_structure(['id' => type_integer()], ['nickname' => type_string()]),
            type_structure(['id' => type_integer(), 'nickname' => type_string()]),
            true,
        ];

        yield 'nested structure with an incompatible element' => [
            type_structure(['id' => type_integer()]),
            type_structure(['id' => type_string()]),
            false,
        ];

        yield 'nested list of matching elements' => [
            type_list(type_integer()),
            type_list(type_integer()),
            true,
        ];

        yield 'nested list of different elements' => [
            type_list(type_integer()),
            type_list(type_string()),
            false,
        ];
    }

    /**
     * @param Type<mixed> $declared
     * @param Type<mixed> $given
     */
    #[DataProvider('provideCases')]
    public function test_element_compatibility(Type $declared, Type $given, bool $expected): void
    {
        static::assertSame($expected, (new ElementCompatibility())->isCompatible('data.element', $declared, $given));
    }
}
