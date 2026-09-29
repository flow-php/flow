<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\array_get_collection;
use function Flow\ETL\DSL\array_get_collection_first;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_get_collection;
use function Flow\ETL\DSL\structure_get_collection_first;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayGetCollectionTest extends FlowTestCase
{
    public function test_array_get_collection_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGetCollection function failed to evaluate parameters');

        $context = flow_context(config());

        (new FunctionContext($context))->eval(
            array_get_collection(ref('invalid_entry'), ['id']),
            ['invalid_entry' => 1],
            schema(int_schema('invalid_entry')),
        );
    }

    public function test_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGetCollection function failed to evaluate parameters.');

        (new FunctionContext(flow_context()))->eval(
            array_get_collection(ref('invalid_entry'), ['id']),
            ['invalid_entry' => 1],
            schema(int_schema('invalid_entry')),
        );
    }

    public function test_getting_keys_from_simple_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGetCollection function failed to evaluate parameters.');

        $rows = RowsMother::arrayEntry();

        (new FunctionContext(flow_context()))->eval(
            array_get_collection(ref('array_entry'), ['id']),
            $rows->values(0),
            $rows->schema(),
        );
    }

    public function test_getting_specific_keys_from_collection_of_array(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'status' => 'PENDING'],
                ['id' => 2, 'status' => 'NEW'],
            ],
            (new FunctionContext(flow_context()))->eval(
                array_get_collection(ref('array_entry'), ['id', 'status']),
                [
                    'array_entry' => [
                        [
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                        [
                            'id' => 2,
                            'status' => 'NEW',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                    ],
                ],
                schema(list_schema(
                    'array_entry',
                    type_list(type_structure([
                        'id' => type_integer(),
                        'status' => type_string(),
                        'enabled' => type_boolean(),
                        'array' => type_structure(['foo' => type_string()]),
                    ])),
                )),
            ),
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array(): void
    {
        static::assertEquals(
            [
                'parent_id' => 1,
            ],
            (new FunctionContext(flow_context()))->eval(
                ref('array_entry')->arrayGetCollectionFirst('parent_id'),
                [
                    'array_entry' => [
                        [
                            'parent_id' => 1,
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                        [
                            'parent_id' => 1,
                            'id' => 2,
                            'status' => 'NEW',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                    ],
                ],
                schema(list_schema(
                    'array_entry',
                    type_list(type_structure([
                        'parent_id' => type_integer(),
                        'id' => type_integer(),
                        'status' => type_string(),
                        'enabled' => type_boolean(),
                        'array' => type_structure(['foo' => type_string()]),
                    ])),
                )),
            ),
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array_when_first_index_does_not_exists(): void
    {
        static::assertEquals(
            [
                'parent_id' => 1,
            ],
            (new FunctionContext(flow_context()))->eval(
                array_get_collection_first(ref('array_entry'), 'parent_id'),
                [
                    'array_entry' => [
                        2 => [
                            'parent_id' => 1,
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                        3 => [
                            'parent_id' => 1,
                            'id' => 2,
                            'status' => 'NEW',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                    ],
                ],
                schema(map_schema('array_entry', type_map(type_integer(), type_structure([
                    'parent_id' => type_integer(),
                    'id' => type_integer(),
                    'status' => type_string(),
                    'enabled' => type_boolean(),
                    'array' => type_structure(['foo' => type_string()]),
                ])))),
            ),
        );
    }

    /**
     * @param list<string> $keys
     * @param list<array<array-key, mixed>> $collection
     * @param list<array<array-key, mixed>> $expected
     */
    #[TestWith([['a.b', 'c'], [['a.b' => 1, 'c' => 2]], [['a.b' => 1, 'c' => 2]]])]
    #[TestWith([['k,l'], [['k,l' => 1]], [['k,l' => 1]]])]
    #[TestWith([['a.b'], [['a' => ['b' => 1]]], [['a.b' => null]]])]
    public function test_keys_are_literal_keys(array $keys, array $collection, array $expected): void
    {
        static::assertSame($expected, (new FunctionContext(flow_context()))->eval(
            array_get_collection(ref('array_entry'), $keys),
            [
                'array_entry' => $collection,
            ],
            schema(json_schema('array_entry')),
        ));
    }

    public function test_a_non_scalar_key_fails(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGetCollection function failed to evaluate parameters.');

        (new FunctionContext(flow_context()))->eval(array_get_collection(ref('array_entry'), [[
            'x',
        ]]), ['array_entry' => [['x' => 1]]], schema(list_schema('array_entry', type_list(type_structure(['x' => type_integer()])))));
    }

    public function test_an_empty_element_reads_null_keys(): void
    {
        static::assertSame(
            [['id' => null], ['id' => null]],
            (new FunctionContext(flow_context()))->eval(
                array_get_collection(ref('array_entry'), ['id']),
                [
                    'array_entry' => [['name' => 'a'], []],
                ],
                schema(json_schema('array_entry')),
            ),
        );
    }

    public function test_structure_get_collection_is_an_alias_of_array_get_collection(): void
    {
        static::assertEquals(
            array_get_collection(ref('array_entry'), ['id']),
            structure_get_collection(ref('array_entry'), ['id']),
        );
    }

    public function test_structure_get_collection_first_is_an_alias_of_array_get_collection_first(): void
    {
        static::assertEquals(
            array_get_collection_first(ref('array_entry'), 'id', 'name'),
            structure_get_collection_first(ref('array_entry'), 'id', 'name'),
        );
    }
}
