<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use ArrayObject;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_mixed;

final class ValueColumnTest extends TestCase
{
    public function test_concat_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'An untyped function result (mixed) cannot be concatenated; it exists only inside function evaluation',
        );

        (new ValueColumn([1]))->concat(new ValueColumn([2]));
    }

    public function test_encode_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'An untyped function result (mixed) cannot be encoded; it exists only inside function evaluation',
        );

        (new ValueColumn([1]))->encode();
    }

    public function test_reads_values_as_given(): void
    {
        $object = new ArrayObject([1]);
        $column = new ValueColumn([['a' => 1], null, $object, 'x']);

        static::assertEquals(type_mixed(), $column->type());
        static::assertSame(4, $column->count());
        static::assertSame(1, $column->nullCount());
        static::assertTrue($column->isNull(1));
        static::assertFalse($column->isNull(0));
        static::assertSame(['a' => 1], $column->at(0));
        static::assertSame($object, $column->value(2));
        static::assertSame([['a' => 1], null, $object, 'x'], $column->physicals());
        static::assertSame([['a' => 1], null, $object, 'x'], $column->values());
    }

    public function test_slice_and_take(): void
    {
        $column = new ValueColumn(['a', null, 'c', 'd']);

        static::assertSame([null, 'c'], $column->slice(1, 2)->values());
        static::assertSame(['d', 'a', 'd'], $column->take([3, 0, 3])->values());
        static::assertSame(0, $column->take([])->count());
    }

    public function test_with_type_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'An untyped function result (mixed) cannot be retyped to integer; it exists only inside function evaluation',
        );

        (new ValueColumn([1]))->withType(type_integer());
    }
}
