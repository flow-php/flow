<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\array_key_rename;
use function Flow\ETL\DSL\ref;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayKeyRenameTest extends TestCase
{
    public function test_renames_array_entry_keys_in_multiple_array_entry() : void
    {
        $row = Row::create(
            Entry::array('customer', [
                'first' => 'John',
                'last' => 'Snow',
            ]),
            Entry::array('shipping', [
                'address' => [
                    'line' => '3644 Clement Street',
                    'city' => 'Atalanta',
                ],
                'estimated_delivery_date' => new \DateTimeImmutable('2023-04-01 10:00:00 UTC'),
            ]),
        );

        $this->assertEquals(
            [
                'first_name' => 'John',
                'last' => 'Snow',
            ],
            array_key_rename(ref('customer'), 'first', 'first_name')->eval($row)
        );

        $this->assertEquals(
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
        $row = Row::create(
            Entry::array('array_entry', [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ]),
        );

        $this->assertEquals(
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
        $row = Row::create(
            Entry::array('array_entry', [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ]),
        );

        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        array_key_rename(ref('array_entry'), 'invalid_path', 'new_name')->eval($row);
    }

    public function test_for_not_array_entry() : void
    {
        $row = Row::create(
            Entry::integer('integer_entry', 1),
        );

        $this->assertNull(array_key_rename(ref('integer_entry'), 'invalid_path', 'new_name')->eval($row));
    }
}
