<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ArrayDotRenameTransformerTest extends TestCase
{
    public function test_renames_array_entry_keys_in_multiple_array_entry() : void
    {
        $transformer = Transform::chain(
            Transform::array_rename_keys('customer', 'first', 'first_name'),
            Transform::array_rename_keys('customer', 'last', 'last_name'),
            Transform::array_rename_keys('shipping', 'address.line', 'street'),
        );

        $rows = $transformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('customer', [
                        'first' => 'John',
                        'last' => 'Snow',
                    ]),
                    new Row\Entry\ArrayEntry('shipping', [
                        'address' => [
                            'line' => '3644 Clement Street',
                            'city' => 'Atalanta',
                        ],
                        'estimated_delivery_date' => new \DateTimeImmutable('2023-04-01 10:00:00 UTC'),
                    ]),
                ),
            ),
        );

        $this->assertEquals(
            [
                [
                    'customer' => [
                        'first_name' => 'John',
                        'last_name' => 'Snow',
                    ],
                    'shipping' => [
                        'address' => [
                            'street' => '3644 Clement Street',
                            'city' => 'Atalanta',
                        ],
                        'estimated_delivery_date' => new \DateTimeImmutable('2023-04-01 10:00:00 UTC'),
                    ],
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_renames_array_entry_keys_in_single_array_entry() : void
    {
        $transformer = Transform::array_rename_keys('array_entry', 'array.foo', 'new_name');

        $rows = $transformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ]),
                ),
            ),
        );

        $this->assertEquals(
            [
                [
                    'array_entry' => [
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'array' => ['new_name' => 'bar'],
                    ],
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_throws_exception_for_invalid_path() : void
    {
        $transformer = Transform::array_rename_keys('array_entry', 'invalid_path', 'new_name');

        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        $transformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ]),
                ),
            ),
        );
    }

    public function test_throws_exception_for_not_array_entry() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('integer_entry is not ArrayEntry but Flow\ETL\Row\Entry\IntegerEntry');

        $transformer = Transform::array_rename_keys('integer_entry', 'invalid_path', 'new_name');

        $transformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('integer_entry', 1),
                ),
            ),
        );
    }
}
