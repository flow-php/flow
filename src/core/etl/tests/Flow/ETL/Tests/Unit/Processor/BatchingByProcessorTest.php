<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\Types\Value\Uuid;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;
use function iterator_to_array;

final class BatchingByProcessorTest extends FlowTestCase
{
    public function test_batching_by_processor_forwards_stop_to_its_upstream(): void
    {
        $upstream = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))->withBatchSize(1);
        $processed = (new BatchingByProcessor(ref('id')))->process($upstream->extract(flow_context()), flow_context());

        static::assertTrue($processed->valid());

        $processed->send(Signal::STOP);

        static::assertFalse($processed->valid());
        static::assertSame(2, $upstream->batchesYielded);
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(str_schema('group'), int_schema('id'));

        static::assertEquals($input, (new BatchingByProcessor(ref('group')))->bind($input)->output);
    }

    public function test_groups_rows_by_column_value(): void
    {
        $processor = new BatchingByProcessor(ref('group'));
        $generator = (static function () {
            yield array_to_rows(
                [
                    ['group' => 'a', 'id' => 1],
                    ['group' => 'a', 'id' => 2],
                    ['group' => 'b', 'id' => 3],
                    ['group' => 'b', 'id' => 4],
                ],
                schema(str_schema('group'), int_schema('id')),
            );
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(2, $result);
        static::assertInstanceOf(Rows::class, $result[0]);
        static::assertInstanceOf(Rows::class, $result[1]);
        static::assertCount(2, $result[0]);
        static::assertCount(2, $result[1]);
        static::assertEquals('a', $result[0]->column('group')->value(0));
        static::assertEquals('b', $result[1]->column('group')->value(0));
    }

    public function test_an_empty_batch_is_skipped_without_reading_the_key(): void
    {
        $generator = (static function () {
            yield array_to_rows([], schema(int_schema('id')));
        })();

        static::assertSame(
            [],
            iterator_to_array((new BatchingByProcessor(ref('group')))->process($generator, flow_context())),
        );
    }

    public function test_handles_empty_input(): void
    {
        $processor = new BatchingByProcessor(ref('group'));
        $generator = (static function () {
            yield from [];
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(0, $result);
    }

    public function test_handles_single_group(): void
    {
        $processor = new BatchingByProcessor(ref('group'));
        $generator = (static function () {
            yield array_to_rows(
                [['group' => 'a', 'id' => 1], ['group' => 'a', 'id' => 2]],
                schema(str_schema('group'), int_schema('id')),
            );
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);
    }

    public function test_respects_min_size(): void
    {
        $processor = new BatchingByProcessor(ref('group'), minSize: 3);
        $generator = (static function () {
            yield array_to_rows(
                [
                    ['group' => 'a', 'id' => 1],
                    ['group' => 'a', 'id' => 2],
                    ['group' => 'b', 'id' => 3],
                    ['group' => 'b', 'id' => 4],
                ],
                schema(str_schema('group'), int_schema('id')),
            );
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(1, $result);
        static::assertCount(4, $result[0]);
    }

    public function test_throws_exception_for_invalid_min_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Minimum batch size must be greater than 0');
        // @mago-ignore analysis:invalid-argument
        new BatchingByProcessor(ref('group'), minSize: 0);
    }

    public function test_equal_datetimes_stay_in_one_batch(): void
    {
        $generator = (static function () {
            yield array_to_rows(
                [
                    ['dt' => new DateTimeImmutable('2024-01-01 00:00:00'), 'id' => 1],
                    ['dt' => new DateTimeImmutable('2024-01-01 00:00:00'), 'id' => 2],
                    ['dt' => new DateTimeImmutable('2024-01-02 00:00:00'), 'id' => 3],
                ],
                schema(datetime_schema('dt'), int_schema('id')),
            );
            yield array_to_rows(
                [['dt' => new DateTimeImmutable('2024-01-02 00:00:00'), 'id' => 4]],
                schema(datetime_schema('dt'), int_schema('id')),
            );
        })();

        $batches = iterator_to_array((new BatchingByProcessor(ref('dt')))->process($generator, flow_context()), false);

        static::assertSame(
            [[1, 2], [3, 4]],
            array_map(static fn(Rows $rows): array => $rows->column('id')->values(), $batches),
        );
    }

    public function test_equal_uuids_stay_in_one_batch(): void
    {
        $generator = (static function () {
            yield array_to_rows(
                [
                    ['u' => new Uuid('00000000-0000-4000-8000-000000000001'), 'id' => 1],
                    ['u' => new Uuid('00000000-0000-4000-8000-000000000001'), 'id' => 2],
                    ['u' => new Uuid('00000000-0000-4000-8000-000000000002'), 'id' => 3],
                ],
                schema(uuid_schema('u'), int_schema('id')),
            );
        })();

        $batches = iterator_to_array((new BatchingByProcessor(ref('u')))->process($generator, flow_context()), false);

        static::assertSame(
            [[1, 2], [3]],
            array_map(static fn(Rows $rows): array => $rows->column('id')->values(), $batches),
        );
    }

    public function test_a_batch_under_another_schema_is_conformed_to_the_first(): void
    {
        $generator = (static function () {
            yield array_to_rows([['g' => 'a', 'name' => 'x']], schema(str_schema('g'), str_schema('name', true)));
            yield array_to_rows([['g' => 'a']], schema(str_schema('g')));
        })();

        $batches = iterator_to_array((new BatchingByProcessor(ref('g')))->process($generator, flow_context()), false);

        static::assertSame([['g' => 'a', 'name' => 'x'], ['g' => 'a', 'name' => null]], $batches[0]->toArray());
    }
}
