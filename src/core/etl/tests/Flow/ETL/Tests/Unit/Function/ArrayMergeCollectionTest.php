<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_merge_collection;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayMergeCollectionTest extends FlowTestCase
{
    public function test_array_merge_collection_in_strict_mode_with_non_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        $row = row(['invalid_entry' => 1]);

        array_merge_collection(ref('invalid_entry'))->eval($row, $context);
    }

    public function test_array_merge_collection_in_strict_mode_with_non_array_elements(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayMergeCollection function requires array elements to be arrays');

        $context = flow_context(config());
        $row = row([
            'array_entry' => [
                ['foo' => 'bar'],
                1,
            ],
        ]);

        array_merge_collection(ref('array_entry'))->eval($row, $context);
    }

    public function test_attempt_of_merging_collection_where_not_every_element_is_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayMergeCollection function requires array elements to be arrays');

        $row = row([
            'array_entry' => [
                ['foo' => 'bar'],
                1,
            ],
        ]);

        array_merge_collection(ref('array_entry'))->eval($row, flow_context());
    }

    public function test_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $row = row(['invalid_entry' => 1]);

        array_merge_collection(ref('invalid_entry'))->eval($row, flow_context());
    }

    public function test_merging_collection_of_arrays(): void
    {
        $row = row([
            'array_entry' => [
                [
                    1,
                ],
                [
                    2,
                ],
                [],
            ],
        ]);

        static::assertEquals([1, 2], array_merge_collection(ref('array_entry'))->eval($row, flow_context()));
    }

    public function test_a_list_of_structures_declares_the_structure(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('collection')->arrayMergeCollection(),
            schema(list_schema(
                'collection',
                type_list(type_structure(['a' => structure_element('a', type_integer(), optional: true)])),
            )),
        );

        static::assertSame('structure{a?: integer}', $resolved->returns()->toString());
    }

    public function test_an_interleaved_structure_keeps_its_order_and_every_field_comes_out_optional(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('collection')->arrayMergeCollection(),
            schema(list_schema(
                'collection',
                type_list(type_structure([
                    'z' => type_integer(),
                    'a' => structure_element('a', type_string(), optional: true),
                    'b' => type_integer(),
                ])),
            )),
        );

        static::assertSame('structure{z?: integer, a?: string, b?: integer}', $resolved->returns()->toString());
    }
}
