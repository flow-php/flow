<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_get_collection;
use function Flow\ETL\DSL\array_get_collection_first;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayGetCollectionTest extends FlowTestCase
{
    public function test_array_get_collection_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGetCollection function failed to evaluate parameters');

        $context = flow_context(config());
        $row = row(['invalid_entry' => 1]);

        array_get_collection(ref('invalid_entry'), ['id'])->eval($row, $context);
    }

    public function test_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGetCollection function failed to evaluate parameters.');

        $row = row(['invalid_entry' => 1]);

        array_get_collection(ref('invalid_entry'), ['id'])->eval($row, flow_context());
    }

    public function test_getting_keys_from_simple_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGetCollection function failed to evaluate parameters.');

        $row = row([
            'array_entry' => [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ],
        ]);

        array_get_collection(ref('array_entry'), ['id'])->eval($row, flow_context());
    }

    public function test_getting_specific_keys_from_collection_of_array(): void
    {
        $row = row([
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
        ]);

        static::assertEquals(
            [
                ['id' => 1, 'status' => 'PENDING'],
                ['id' => 2, 'status' => 'NEW'],
            ],
            array_get_collection(ref('array_entry'), ['id', 'status'])->eval($row, flow_context()),
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array(): void
    {
        $row = row([
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
        ]);

        static::assertEquals(
            [
                'parent_id' => 1,
            ],
            ref('array_entry')->arrayGetCollectionFirst('parent_id')->eval($row, flow_context()),
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array_when_first_index_does_not_exists(): void
    {
        $row = row([
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
        ]);

        static::assertEquals(
            [
                'parent_id' => 1,
            ],
            array_get_collection_first(ref('array_entry'), 'parent_id')->eval($row, flow_context()),
        );
    }
}
