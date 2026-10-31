<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class OffsetProcessorTest extends FlowTestCase
{
    public function test_offset_processor_forwards_stop_to_its_upstream(): void
    {
        $upstream = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))->withBatchSize(1);
        $processed = (new OffsetProcessor(1))->process(
            FlowTestCase::extracted($upstream, flow_context()->backend(), new Criteria()),
            flow_context()->backend(),
        );

        static::assertTrue($processed->valid());

        $processed->send(Signal::STOP);

        static::assertFalse($processed->valid());
        static::assertSame(2, $upstream->batchesYielded);
    }

    public function test_an_empty_batch_inside_the_offset_is_skipped(): void
    {
        $result = iterator_to_array(
            (new OffsetProcessor(2))->process(
                (static function () {
                    yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                    yield rows(schema());
                    yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertEquals([array_to_rows([['id' => 3]], schema(int_schema('id')))], $result);
    }

    public function test_an_offset_equal_to_the_rows_yields_no_batch(): void
    {
        $result = iterator_to_array((new OffsetProcessor(3))->process(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));

        static::assertSame([], $result);
    }

    public function test_an_offset_inside_the_first_batch_keeps_the_later_batches_whole(): void
    {
        $result = iterator_to_array(
            (new OffsetProcessor(1))->process(
                (static function () {
                    yield array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertEquals(
            [
                array_to_rows([['id' => 2], ['id' => 3]], schema(int_schema('id'))),
                array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id'))),
            ],
            $result,
        );
    }

    public function test_an_offset_spanning_batches_cuts_the_batch_it_ends_in(): void
    {
        $result = iterator_to_array(
            (new OffsetProcessor(3))->process(
                (static function () {
                    yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 3], ['id' => 4], ['id' => 5]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 6]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertEquals(
            [
                array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id'))),
                array_to_rows([['id' => 6]], schema(int_schema('id'))),
            ],
            $result,
        );
    }

    public function test_an_offset_skips_whole_batches(): void
    {
        $result = iterator_to_array(
            (new OffsetProcessor(4))->process(
                (static function () {
                    yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 5], ['id' => 6]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertEquals([array_to_rows([['id' => 5], ['id' => 6]], schema(int_schema('id')))], $result);
    }

    public function test_keeps_every_column_of_the_rows_after_the_offset(): void
    {
        $schema = schema(int_schema('id'), bool_schema('active'));
        $result = iterator_to_array(
            (new OffsetProcessor(1))->process(
                (static function () use ($schema) {
                    yield array_to_rows([
                        ['id' => 1, 'active' => true],
                        ['id' => 2, 'active' => false],
                        ['id' => 3, 'active' => true],
                    ], $schema);
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertEquals(
            [array_to_rows([['id' => 2, 'active' => false], ['id' => 3, 'active' => true]], $schema)],
            $result,
        );
    }

    public function test_no_input_batches_yield_no_batch(): void
    {
        $result = iterator_to_array((new OffsetProcessor(5))->process(
            (static function () {
                yield from [];
            })(),
            flow_context()->backend(),
        ));

        static::assertSame([], $result);
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'));

        static::assertEquals($input, (new OffsetProcessor(1))->bind($input)->output);
    }

    public function test_a_fresh_instance_skips_the_same_offset(): void
    {
        $processor = new OffsetProcessor(3);
        $fresh = $processor->fresh();

        static::assertNotSame($processor, $fresh);
        static::assertSame(3, $fresh->offset);
    }

    public function test_offset_greater_than_total_rows_yields_nothing(): void
    {
        $processor = new OffsetProcessor(10);
        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()->backend()));
        $totalRows = 0;

        foreach ($result as $batch) {
            $totalRows += $batch->count();
        }

        static::assertSame(0, $totalRows);
    }

    public function test_offset_within_single_batch(): void
    {
        $processor = new OffsetProcessor(1);
        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()->backend()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals(
            [
                ['id' => 2],
                ['id' => 3],
            ],
            $allRows,
        );
    }

    public function test_offset_zero_yields_all_rows(): void
    {
        $processor = new OffsetProcessor(0);
        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()->backend()));
        $totalRows = 0;

        foreach ($result as $batch) {
            $totalRows += $batch->count();
        }

        static::assertSame(2, $totalRows);
    }

    public function test_skips_first_n_rows(): void
    {
        $processor = new OffsetProcessor(2);
        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            yield array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id')));
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()->backend()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals(
            [
                ['id' => 3],
                ['id' => 4],
            ],
            $allRows,
        );
    }

    public function test_throws_exception_for_negative_offset(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0');
        // @mago-ignore analysis:invalid-argument
        new OffsetProcessor(-1);
    }
}
