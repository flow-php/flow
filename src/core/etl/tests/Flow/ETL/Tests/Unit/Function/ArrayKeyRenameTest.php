<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_key_rename;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayKeyRenameTest extends FlowTestCase
{
    public function test_array_key_rename_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayKeyRename function requires non-null array, path, and new name');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        $row = row(int_entry('integer_entry', 1));

        array_key_rename(ref('integer_entry'), 'invalid_path', 'new_name')->eval($row, $context);
    }

    public function test_for_not_array_entry(): void
    {
        $row = row(int_entry('integer_entry', 1));

        static::assertNull(array_key_rename(ref('integer_entry'), 'invalid_path', 'new_name')->eval(
            $row,
            flow_context(),
        ));
    }

    public function test_renames_array_entry_keys_in_multiple_array_entry(): void
    {
        $row = row(
            json_entry('customer', [
                'first' => 'John',
                'last' => 'Snow',
            ]),
            json_entry('shipping', [
                'address' => [
                    'line' => '3644 Clement Street',
                    'city' => 'Atalanta',
                ],
                'estimated_delivery_date' => '2023-04-01T10:00:00+00:00',
            ]),
        );

        static::assertEquals(
            [
                'first_name' => 'John',
                'last' => 'Snow',
            ],
            array_key_rename(ref('customer'), 'first', 'first_name')->eval($row, flow_context()),
        );

        static::assertEquals(
            [
                'address' => [
                    'street' => '3644 Clement Street',
                    'city' => 'Atalanta',
                ],
                'estimated_delivery_date' => '2023-04-01T10:00:00+00:00',
            ],
            array_key_rename(ref('shipping'), 'address.line', 'street')->eval($row, flow_context()),
        );
    }

    public function test_renames_array_entry_keys_in_single_array_entry(): void
    {
        $row = row(json_entry('array_entry', [
            'id' => 1,
            'status' => 'PENDING',
            'enabled' => true,
            'array' => ['foo' => 'bar'],
        ]));

        static::assertEquals(
            [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['new_name' => 'bar'],
            ],
            array_key_rename(ref('array_entry'), 'array.foo', 'new_name')->eval($row, flow_context()),
        );
    }

    public function test_throws_exception_for_invalid_path(): void
    {
        $row = row(json_entry('array_entry', [
            'id' => 1,
            'status' => 'PENDING',
            'enabled' => true,
            'array' => ['foo' => 'bar'],
        ]));

        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        array_key_rename(ref('array_entry'), 'invalid_path', 'new_name')->eval($row, flow_context());
    }
}
