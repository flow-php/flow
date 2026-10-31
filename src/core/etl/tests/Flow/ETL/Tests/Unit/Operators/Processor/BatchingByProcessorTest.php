<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\Processor\BatchingByProcessor;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\VaryingBatchesExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Rows;
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
    public function test_a_later_batch_that_widens_the_shape_is_refused(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "extra" (row 0) is not declared by the schema');

        iterator_to_array(
            (new BatchingByProcessor(ref('g')))->process(
                FlowTestCase::extracted(
                    new VaryingBatchesExtractor(
                        array_to_rows([['g' => 1]], schema(int_schema('g'))),
                        array_to_rows([['g' => 1, 'extra' => 'x']], schema(int_schema('g'), str_schema('extra'))),
                    ),
                    flow_context()->backend(),
                    new Criteria(),
                ),
                flow_context()->backend(),
            ),
            false,
        );
    }

    public function test_batching_by_processor_forwards_stop_to_its_upstream(): void
    {
        $upstream = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))->withBatchSize(1);
        $processed = (new BatchingByProcessor(ref('id')))->process(
            FlowTestCase::extracted($upstream, flow_context()->backend(), new Criteria()),
            flow_context()->backend(),
        );

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
        $result = iterator_to_array((new BatchingByProcessor(ref('group')))->process(
            (static function () {
                yield array_to_rows(
                    [
                        ['group' => 'a', 'id' => 1],
                        ['group' => 'a', 'id' => 2],
                        ['group' => 'b', 'id' => 3],
                        ['group' => 'b', 'id' => 4],
                    ],
                    schema(str_schema('group'), int_schema('id')),
                );
            })(),
            flow_context()->backend(),
        ));
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
        static::assertSame(
            [],
            iterator_to_array((new BatchingByProcessor(ref('group')))->process(
                (static function () {
                    yield array_to_rows([], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_handles_empty_input(): void
    {
        static::assertCount(
            0,
            iterator_to_array((new BatchingByProcessor(ref('group')))->process(
                (static function () {
                    yield from [];
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_handles_single_group(): void
    {
        $result = iterator_to_array((new BatchingByProcessor(ref('group')))->process(
            (static function () {
                yield array_to_rows(
                    [['group' => 'a', 'id' => 1], ['group' => 'a', 'id' => 2]],
                    schema(str_schema('group'), int_schema('id')),
                );
            })(),
            flow_context()->backend(),
        ));
        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);
    }

    public function test_respects_min_size(): void
    {
        $result = iterator_to_array((new BatchingByProcessor(ref('group'), minSize: 3))->process(
            (static function () {
                yield array_to_rows(
                    [
                        ['group' => 'a', 'id' => 1],
                        ['group' => 'a', 'id' => 2],
                        ['group' => 'b', 'id' => 3],
                        ['group' => 'b', 'id' => 4],
                    ],
                    schema(str_schema('group'), int_schema('id')),
                );
            })(),
            flow_context()->backend(),
        ));
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
        static::assertSame(
            [[1, 2], [3, 4]],
            array_map(
                static fn(Rows $rows): array => $rows->column('id')->values(),
                iterator_to_array(
                    (new BatchingByProcessor(ref('dt')))->process(
                        (static function () {
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
                        })(),
                        flow_context()->backend(),
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_equal_uuids_stay_in_one_batch(): void
    {
        static::assertSame(
            [[1, 2], [3]],
            array_map(
                static fn(Rows $rows): array => $rows->column('id')->values(),
                iterator_to_array(
                    (new BatchingByProcessor(ref('u')))->process(
                        (static function () {
                            yield array_to_rows(
                                [
                                    ['u' => new Uuid('00000000-0000-4000-8000-000000000001'), 'id' => 1],
                                    ['u' => new Uuid('00000000-0000-4000-8000-000000000001'), 'id' => 2],
                                    ['u' => new Uuid('00000000-0000-4000-8000-000000000002'), 'id' => 3],
                                ],
                                schema(uuid_schema('u'), int_schema('id')),
                            );
                        })(),
                        flow_context()->backend(),
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_a_batch_under_another_schema_is_conformed_to_the_first(): void
    {
        static::assertSame(
            [['g' => 'a', 'name' => 'x'], ['g' => 'a', 'name' => null]],
            iterator_to_array(
                (new BatchingByProcessor(ref('g')))->process(
                    (static function () {
                        yield array_to_rows(
                            [['g' => 'a', 'name' => 'x']],
                            schema(str_schema('g'), str_schema('name', true)),
                        );
                        yield array_to_rows([['g' => 'a']], schema(str_schema('g')));
                    })(),
                    flow_context()->backend(),
                ),
                false,
            )[0]->toArray(),
        );
    }
}
