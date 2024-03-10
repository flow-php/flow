<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{array_entry, array_exists, array_get, int_entry, ref};
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayGetTest extends TestCase
{
    public function test_array_access_for_not_array_entry() : void
    {
        self::assertNull(array_get(ref('integer_entry'), 'invalid_path')->eval(Row::create(int_entry('integer_entry', 1))));
        self::assertFalse(array_exists(ref('integer_entry'), 'invalid_path')->eval(Row::create(int_entry('integer_entry', 1))));
    }

    public function test_array_accessor_transformer() : void
    {
        $row = Row::create(
            array_entry('array_entry', [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ]),
        );
        self::assertEquals('bar', array_get(ref('array_entry'), 'array.foo')->eval($row));
        self::assertTrue(array_exists(ref('array_entry'), 'array.foo')->eval($row));
    }

    public function test_array_accessor_transformer_with_invalid_and_without_strict_path() : void
    {
        $row = Row::create(
            array_entry('array_entry', [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'datetime' => new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                'array' => ['foo' => 'bar'],
            ]),
        );
        self::assertNull(array_get(ref('array_entry'), '?invalid_path')->eval($row));
        self::assertTrue(array_exists(ref('array_entry'), '?invalid_path')->eval($row));
        self::assertFalse(array_exists(ref('array_entry'), 'invalid_path')->eval($row));
    }

    public function test_array_accessor_transformer_with_invalid_but_strict_path() : void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        array_get(ref('array_entry'), 'invalid_path')->eval(
            Row::create(
                array_entry('array_entry', [
                    'id' => 1,
                    'status' => 'PENDING',
                    'enabled' => true,
                    'datetime' => new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                    'array' => ['foo' => 'bar'],
                ]),
            ),
        );
    }
}
