<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{array_key_rename, int_entry, json_entry, ref};
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayKeyRenameTest extends FlowTestCase
{
    public function test_for_not_array_entry() : void
    {
        $row = row(int_entry('integer_entry', 1));

        self::assertNull(array_key_rename(ref('integer_entry'), 'invalid_path', 'new_name')->eval($row));
    }

    public function test_renames_array_entry_keys_in_multiple_array_entry() : void
    {
        $row = row(json_entry('customer', [
            'first' => 'John',
            'last' => 'Snow',
        ]), json_entry('shipping', [
            'address' => [
                'line' => '3644 Clement Street',
                'city' => 'Atalanta',
            ],
            'estimated_delivery_date' => new \DateTimeImmutable('2023-04-01 10:00:00 UTC'),
        ]));

        self::assertEquals(
            [
                'first_name' => 'John',
                'last' => 'Snow',
            ],
            array_key_rename(ref('customer'), 'first', 'first_name')->eval($row)
        );

        self::assertEquals(
            [
                'address' => [
                    'street' => '3644 Clement Street',
                    'city' => 'Atalanta',
                ],
                'estimated_delivery_date' => new \DateTimeImmutable('2023-04-01 10:00:00 UTC'),

            ],
            array_key_rename(ref('shipping'), 'address.line', 'street')->eval($row)
        );
    }

    public function test_renames_array_entry_keys_in_single_array_entry() : void
    {
        $row = row(json_entry('array_entry', [
            'id' => 1,
            'status' => 'PENDING',
            'enabled' => true,
            'array' => ['foo' => 'bar'],
        ]));

        self::assertEquals(
            [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['new_name' => 'bar'],
            ],
            array_key_rename(ref('array_entry'), 'array.foo', 'new_name')->eval($row)
        );
    }

    public function test_throws_exception_for_invalid_path() : void
    {
        $row = row(json_entry('array_entry', [
            'id' => 1,
            'status' => 'PENDING',
            'enabled' => true,
            'array' => ['foo' => 'bar'],
        ]));

        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        array_key_rename(ref('array_entry'), 'invalid_path', 'new_name')->eval($row);
    }
}
