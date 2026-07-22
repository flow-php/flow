<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\RowsBuffer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;

final class RowsBufferTest extends FlowTestCase
{
    public function test_add_returns_a_full_batch_and_resets(): void
    {
        $buffer = new RowsBuffer(2);

        static::assertNull($buffer->add(row(int_entry('id', 1))));

        $batch = $buffer->add(row(int_entry('id', 2)));

        static::assertNotNull($batch);
        static::assertSame([1, 2], $batch->reduceToArray('id'));
        static::assertNull($buffer->add(row(int_entry('id', 3))));
    }

    public function test_add_returns_null_until_size_is_reached(): void
    {
        $buffer = new RowsBuffer(3);

        static::assertNull($buffer->add(row(int_entry('id', 1))));
        static::assertNull($buffer->add(row(int_entry('id', 2))));
    }

    public function test_flush_returns_null_when_empty(): void
    {
        static::assertNull((new RowsBuffer(2))->flush());
    }

    public function test_flush_returns_the_remainder(): void
    {
        $buffer = new RowsBuffer(3);
        $buffer->add(row(int_entry('id', 1)));

        $batch = $buffer->flush();

        static::assertNotNull($batch);
        static::assertSame([1], $batch->reduceToArray('id'));
        static::assertNull($buffer->flush());
    }

    public function test_throws_when_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Buffer size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new RowsBuffer(0);
    }
}
