<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows\RowsBuffer;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowsBufferTest extends FlowTestCase
{
    public function test_released_batches_are_built_through_the_given_backend(): void
    {
        $backend = new SpyBackend();
        $buffer = new RowsBuffer(schema(int_schema('id')), $backend, 1);

        $buffer->appendFrom(array_to_rows([['id' => 1]], schema(int_schema('id'))), 0);

        static::assertGreaterThanOrEqual(1, $backend->builders());
    }

    public function test_a_released_batch_carries_the_buffer_schema(): void
    {
        $schema = schema(int_schema('id', nullable: true));
        $buffer = new RowsBuffer($schema, new PhpBackend(), 1);

        static::assertTrue(
            $buffer
                ->appendFrom(array_to_rows([['id' => 1]], $schema), 0)
                ?->schema()
                ->isSame($schema),
        );
    }

    public function test_append_from_checks_every_row(): void
    {
        $this->expectException(SchemaMismatchException::class);

        (new RowsBuffer(schema(int_schema('id')), new PhpBackend(), 1))->appendFrom(array_to_rows([[
            'id' => 'x',
        ]], schema(str_schema('id'))), 0);
    }

    public function test_append_from_returns_a_full_batch_and_resets(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
        $buffer = new RowsBuffer(schema(int_schema('id')), new PhpBackend(), 2);

        static::assertNull($buffer->appendFrom($rows, 0));

        $batch = $buffer->appendFrom($rows, 1);

        static::assertNotNull($batch);
        static::assertSame([1, 2], $batch->reduceToArray('id'));
        static::assertNull($buffer->appendFrom($rows, 2));
    }

    public function test_append_from_returns_null_until_size_is_reached(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        $buffer = new RowsBuffer(schema(int_schema('id')), new PhpBackend(), 3);

        static::assertNull($buffer->appendFrom($rows, 0));
        static::assertNull($buffer->appendFrom($rows, 1));
    }

    public function test_append_returns_a_full_batch(): void
    {
        $buffer = new RowsBuffer(schema(int_schema('id')), new PhpBackend(), 2);

        static::assertNull($buffer->append(['id' => 1]));
        static::assertSame([1, 2], $buffer->append(['id' => 2])?->reduceToArray('id'));
    }

    public function test_append_take_overshooting_the_size_is_released_whole(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
        $buffer = new RowsBuffer(schema(int_schema('id')), new PhpBackend(), 2);

        static::assertSame([3, 1, 2], $buffer->appendTake($rows, [2, 0, 1])?->reduceToArray('id'));
        static::assertNull($buffer->flush());
    }

    public function test_flush_returns_null_when_empty(): void
    {
        static::assertNull((new RowsBuffer(schema(int_schema('id')), new PhpBackend(), 2))->flush());
    }

    public function test_flush_returns_the_remainder(): void
    {
        $buffer = new RowsBuffer(schema(int_schema('id')), new PhpBackend(), 3);
        $buffer->append(['id' => 1]);

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
        new RowsBuffer(schema(int_schema('id')), new PhpBackend(), 0);
    }
}
