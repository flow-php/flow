<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\RowsBuffer;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;

final class RowsBufferTest extends FlowTestCase
{
    public function test_a_batch_factory_builds_each_released_batch(): void
    {
        // Rows::trusted() takes the rows as given - a value the default gate would refuse passes through
        $buffer = new RowsBuffer(schema(int_schema('id')), 1, Rows::trusted(...));

        static::assertSame([['id' => 'x']], $buffer->add(row(['id' => 'x']))?->toArray());
    }

    public function test_the_default_batch_checks_every_row(): void
    {
        $this->expectException(SchemaMismatchException::class);

        (new RowsBuffer(schema(int_schema('id')), 1))->add(row(['id' => 'x']));
    }

    public function test_add_returns_a_full_batch_and_resets(): void
    {
        $buffer = new RowsBuffer(schema(int_schema('id')), 2);

        static::assertNull($buffer->add(row(['id' => 1])));

        $batch = $buffer->add(row(['id' => 2]));

        static::assertNotNull($batch);
        static::assertSame([1, 2], $batch->reduceToArray('id'));
        static::assertNull($buffer->add(row(['id' => 3])));
    }

    public function test_add_returns_null_until_size_is_reached(): void
    {
        $buffer = new RowsBuffer(schema(int_schema('id')), 3);

        static::assertNull($buffer->add(row(['id' => 1])));
        static::assertNull($buffer->add(row(['id' => 2])));
    }

    public function test_flush_returns_null_when_empty(): void
    {
        static::assertNull((new RowsBuffer(schema(int_schema('id')), 2))->flush());
    }

    public function test_flush_returns_the_remainder(): void
    {
        $buffer = new RowsBuffer(schema(int_schema('id')), 3);
        $buffer->add(row(['id' => 1]));

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
        new RowsBuffer(schema(int_schema('id')), 0);
    }
}
