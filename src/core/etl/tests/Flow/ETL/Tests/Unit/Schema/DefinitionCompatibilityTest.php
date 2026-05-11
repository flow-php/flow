<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class DefinitionCompatibilityTest extends FlowTestCase
{
    public static function list_compatibility_provider(): \Generator
    {
        yield [list_schema('list', type_list(type_integer())), list_schema('list', type_list(type_integer())), true];
        yield [
            list_schema('list', type_list(type_integer())),
            list_schema('other_list', type_list(type_integer())),
            false,
        ];
        yield [
            list_schema('list', type_list(type_integer())),
            list_schema('list', type_list(type_integer())),
            true,
            false,
        ];

        yield [
            list_schema('list', type_list(type_integer()), true),
            list_schema('list', type_list(type_integer())),
            true,
        ];
        yield [
            list_schema('list', type_list(type_integer()), true),
            list_schema('list', type_list(type_integer()), true),
            true,
        ];

        yield [
            list_schema('list', type_list(type_optional(type_integer()))),
            list_schema('list', type_list(type_integer())),
            true,
        ];
        yield [
            list_schema('list', type_list(type_integer())),
            list_schema('list', type_list(type_optional(type_integer()))),
            false,
        ];

        yield [list_schema('list', type_list(type_integer())), list_schema('list', type_list(type_string())), false];

        yield [
            list_schema('list', type_list(type_integer())),
            list_schema('list', type_list(type_optional(type_integer()))),
            false,
        ];

        yield [
            list_schema('list', type_list(type_list(type_integer()))),
            list_schema('list', type_list(type_list(type_integer()))),
            true,
        ];
        yield [
            list_schema('list', type_list(type_list(type_optional(type_integer())))),
            list_schema('list', type_list(type_list(type_integer()))),
            true,
        ];
        yield [
            list_schema('list', type_list(type_list(type_integer()))),
            list_schema('list', type_list(type_list(type_optional(type_integer())))),
            false,
        ];
    }

    public static function map_compatibility_provider(): \Generator
    {
        yield [
            map_schema('map', type_map(type_string(), type_integer())),
            map_schema('map', type_map(type_string(), type_integer())),
            true,
        ];
        yield [
            map_schema('map', type_map(type_string(), type_integer())),
            map_schema('different_map', type_map(type_string(), type_integer())),
            false,
        ];
        yield [
            map_schema('map', type_map(type_string(), type_integer())),
            map_schema('map', type_map(type_string(), type_integer()), true),
            false,
        ];
        yield [
            map_schema('map', type_map(type_string(), type_integer()), true),
            map_schema('map', type_map(type_string(), type_integer())),
            true,
        ];

        yield [
            map_schema('map', type_map(type_string(), type_optional(type_integer()))),
            map_schema('map', type_map(type_string(), type_integer())),
            true,
        ];
        yield [
            map_schema('map', type_map(type_string(), type_integer())),
            map_schema('map', type_map(type_string(), type_optional(type_integer()))),
            false,
        ];

        yield [
            map_schema('map', type_map(type_string(), type_integer()), true),
            map_schema('map', type_map(type_string(), type_integer()), true),
            true,
        ];
        yield [
            map_schema('map', type_map(type_string(), type_integer())),
            map_schema('map', type_map(type_string(), type_string())),
            false,
        ];
        yield [
            map_schema('map', type_map(type_string(), type_integer())),
            map_schema('map', type_map(type_string(), type_optional(type_integer()))),
            false,
        ];
        yield [
            map_schema('map', type_map(type_string(), type_optional(type_integer()))),
            map_schema('map', type_map(type_string(), type_integer())),
            true,
        ];
    }

    public static function scalar_types_compatibility_provider(): \Generator
    {
        yield [int_schema('int'), int_schema('int'), true];
        yield [int_schema('int', true), int_schema('int'), true];
        yield [int_schema('int'), int_schema('int', true), false];
        yield [int_schema('int', true), int_schema('int', true), true];
        yield [int_schema('int', true), int_schema('other-int', true), false];

        yield [string_schema('string'), string_schema('string'), true];
        yield [string_schema('string', true), string_schema('string'), true];
        yield [string_schema('string'), string_schema('string', true), false];
        yield [string_schema('string', true), string_schema('string', true), true];
        yield [string_schema('string', true), string_schema('other-string', true), false];
    }

    public static function structure_types_compatibility_provider(): \Generator
    {
        yield [
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            true,
        ];
        yield [
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('structure', type_structure(['id' => type_integer()])),
            false,
        ];
        yield [
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_boolean()])),
            false,
        ];
        yield [
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('different_structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            false,
        ];
        yield [
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()]), true),
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            true,
        ];
        yield [
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()]), true),
            false,
        ];

        yield [
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('structure', type_structure(['different_id' => type_integer(), 'name' => type_string()])),
            false,
        ];

        yield [
            structure_schema('structure', type_structure([
                'id' => type_optional(type_integer()),
                'name' => type_string(),
            ])),
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            true,
        ];

        yield [
            structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_string()])),
            structure_schema('structure', type_structure([
                'id' => type_optional(type_integer()),
                'name' => type_string(),
            ])),
            false,
        ];
    }

    /**
     * @param Definition<mixed> $given
     * @param Definition<mixed> $expected
     */
    #[DataProvider('list_compatibility_provider')]
    public function test_list_type_compatibility(Definition $given, Definition $expected, bool $compatible): void
    {
        static::assertSame($compatible, $given->isCompatible($expected));
    }

    /**
     * @param Definition<mixed> $given
     * @param Definition<mixed> $expected
     */
    #[DataProvider('map_compatibility_provider')]
    public function test_map_type_compatibility(Definition $given, Definition $expected, bool $compatible): void
    {
        static::assertSame($compatible, $given->isCompatible($expected));
    }

    /**
     * @param Definition<mixed> $given
     * @param Definition<mixed> $expected
     */
    #[DataProvider('scalar_types_compatibility_provider')]
    public function test_scalar_type_compatibility(Definition $given, Definition $expected, bool $compatible): void
    {
        static::assertSame($compatible, $given->isCompatible($expected));
    }

    /**
     * @param Definition<mixed> $given
     * @param Definition<mixed> $expected
     */
    #[DataProvider('structure_types_compatibility_provider')]
    public function test_structure_type_compatibility(Definition $given, Definition $expected, bool $compatible): void
    {
        static::assertSame($compatible, $given->isCompatible($expected));
    }
}
