<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArrayGet;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_exists;
use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayGetTest extends FlowTestCase
{
    public function test_constructor_rejects_a_wildcard_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('contains a wildcard');

        new ArrayGet(ref('array'), '*.id');
    }

    public function test_constructor_rejects_a_branch_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('contains a wildcard');

        new ArrayGet(ref('array'), '{a,b}');
    }

    public function test_array_access_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGet function failed to get value from array.');

        array_get(ref('integer_entry'), 'invalid_path')->eval(row(int_entry('integer_entry', 1)), flow_context());
        array_exists(ref('integer_entry'), 'invalid_path')->eval(row(int_entry('integer_entry', 1)), flow_context());
    }

    public function test_array_access_for_not_array_entry_strict_mode(): void
    {
        $context = flow_context();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGet function failed to get value from array');

        array_get(ref('integer_entry'), 'invalid_path')->eval(row(int_entry('integer_entry', 1)), $context);
    }

    public function test_array_accessor_transformer(): void
    {
        $row = row(json_entry('array_entry', [
            'id' => 1,
            'status' => 'PENDING',
            'enabled' => true,
            'array' => ['foo' => 'bar'],
        ]));
        static::assertEquals('bar', array_get(ref('array_entry'), 'array.foo')->eval($row, flow_context()));
        static::assertTrue(array_exists(ref('array_entry'), 'array.foo')->eval($row, flow_context()));
    }

    public function test_array_accessor_transformer_with_invalid_and_without_strict_path(): void
    {
        $row = row(json_entry('array_entry', [
            'id' => 1,
            'status' => 'PENDING',
            'enabled' => true,
            'datetime' => new DateTimeImmutable('2020-01-01 00:00:00 UTC'),
            'array' => ['foo' => 'bar'],
        ]));
        static::assertNull(array_get(ref('array_entry'), '?invalid_path')->eval($row, flow_context()));
        static::assertTrue(array_exists(ref('array_entry'), '?invalid_path')->eval($row, flow_context()));
        static::assertFalse(array_exists(ref('array_entry'), 'invalid_path')->eval($row, flow_context()));
    }

    public function test_array_accessor_transformer_with_invalid_but_strict_path(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        array_get(ref('array_entry'), 'invalid_path')->eval(
            row(json_entry('array_entry', [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'datetime' => new DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                'array' => ['foo' => 'bar'],
            ])),
            flow_context(),
        );
    }
}
