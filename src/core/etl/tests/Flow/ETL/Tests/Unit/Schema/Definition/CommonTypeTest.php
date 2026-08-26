<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\CommonType;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class CommonTypeTest extends FlowTestCase
{
    public static function provide_container_pairs(): Generator
    {
        $containers = [
            'json' => json_schema('c'),
            'list' => list_schema('c', type_list(type_string())),
            'map' => map_schema('c', type_map(type_string(), type_integer())),
            'structure' => structure_schema('c', type_structure(['a' => type_integer()])),
        ];

        foreach ($containers as $leftName => $left) {
            foreach ($containers as $rightName => $right) {
                yield $leftName . ' and ' . $rightName => [$left, $right];
            }
        }
    }

    public static function provide_pairs_without_a_common_container(): Generator
    {
        $scalars = [
            'string' => str_schema('c'),
            'integer' => int_schema('c'),
            'boolean' => bool_schema('c'),
        ];
        $containers = [
            'json' => json_schema('c'),
            'list' => list_schema('c', type_list(type_string())),
        ];

        foreach ($scalars as $leftName => $left) {
            foreach ($scalars as $rightName => $right) {
                yield $leftName . ' and ' . $rightName => [$left, $right];
            }

            foreach ($containers as $rightName => $right) {
                yield $leftName . ' and ' . $rightName => [$left, $right];
                yield $rightName . ' and ' . $leftName => [$right, $left];
            }
        }
    }

    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    #[DataProvider('provide_container_pairs')]
    public function test_two_containers_widen_to_json(Definition $left, Definition $right): void
    {
        static::assertInstanceOf(JsonDefinition::class, (new CommonType())->merge($left, $right));
    }

    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    #[DataProvider('provide_pairs_without_a_common_container')]
    public function test_every_other_pair_widens_to_string(Definition $left, Definition $right): void
    {
        static::assertInstanceOf(StringDefinition::class, (new CommonType())->merge($left, $right));
    }

    public function test_nullability_is_the_or_of_both_sides(): void
    {
        static::assertFalse(
            (new CommonType())
                ->merge(str_schema('c'), int_schema('c'))
                ->isNullable(),
        );
        static::assertTrue(
            (new CommonType())
                ->merge(str_schema('c', nullable: true), int_schema('c'))
                ->isNullable(),
        );
        static::assertTrue(
            (new CommonType())
                ->merge(str_schema('c'), int_schema('c', nullable: true))
                ->isNullable(),
        );
    }

    public function test_metadata_from_both_sides_is_merged(): void
    {
        $merged = (new CommonType())->merge(
            str_schema('c', metadata: Metadata::with('a', 1)),
            int_schema('c', metadata: Metadata::with('b', 2)),
        );

        static::assertTrue($merged->metadata()->has('a'));
        static::assertTrue($merged->metadata()->has('b'));
    }

    public function test_the_result_keeps_the_left_column_name(): void
    {
        static::assertSame(
            'c',
            (new CommonType())
                ->merge(str_schema('c'), int_schema('c'))
                ->entry()
                ->name(),
        );
    }
}
