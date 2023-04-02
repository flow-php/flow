<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\array_exists;
use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\ref;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayGetTest extends TestCase
{
    public function test_array_access_for_not_array_entry() : void
    {
        $this->assertNull(array_get(ref('integer_entry'), 'invalid_path')->eval(Row::create(Entry::int('integer_entry', 1))));
        $this->assertFalse(array_exists(ref('integer_entry'), 'invalid_path')->eval(Row::create(Entry::int('integer_entry', 1))));
    }

    public function test_array_accessor_transformer() : void
    {
        $row = Row::create(
            Entry::array('array_entry', [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ]),
        );
        $this->assertEquals('bar', array_get(ref('array_entry'), 'array.foo')->eval($row));
        $this->assertTrue(array_exists(ref('array_entry'), 'array.foo')->eval($row));
    }

    public function test_array_accessor_transformer_with_invalid_and_without_strict_path() : void
    {
        $row = Row::create(
            Entry::array('array_entry', [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'datetime' => new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                'array' => ['foo' => 'bar'],
            ]),
        );
        $this->assertNull(array_get(ref('array_entry'), '?invalid_path')->eval($row));
        $this->assertFalse(array_exists(ref('array_entry'), '?invalid_path')->eval($row));
    }

    public function test_array_accessor_transformer_with_invalid_but_strict_path() : void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        array_get(ref('array_entry'), 'invalid_path')->eval(
            Row::create(
                Entry::array('array_entry', [
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
