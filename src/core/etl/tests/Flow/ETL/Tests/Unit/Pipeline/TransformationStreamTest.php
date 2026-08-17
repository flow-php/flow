<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use FiberError;
use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Pipeline\TransformationStream;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\select;

final class TransformationStreamTest extends FlowTestCase
{
    public function test_a_sink_failure_propagates_from_drain(): void
    {
        $failure = new RuntimeException('sink exploded');
        $sink = new ThrowingLoader($failure);
        $stream = new TransformationStream(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect()),
            $sink,
            flow_context(config()),
        );

        $stream->feed(rows(row(int_entry('id', 1))));

        $loadsBeforeDrain = $sink->loadsCount;

        try {
            $stream->drain();

            static::fail('Expected the sink failure to propagate out of drain().');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame(0, $loadsBeforeDrain);
        static::assertSame(1, $sink->loadsCount);
    }

    public function test_a_sink_failure_propagates_from_feed(): void
    {
        $failure = new RuntimeException('sink exploded');
        $stream = new TransformationStream(select('id'), new ThrowingLoader($failure), flow_context(config()));

        try {
            $stream->feed(rows(row(int_entry('id', 1))));

            static::fail('Expected the sink failure to propagate out of feed().');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }
    }

    public function test_a_terminated_drive_ignores_later_feeds(): void
    {
        $sink = new SpyLoader();
        $stream = new TransformationStream(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->limit(1)),
            $sink,
            flow_context(config()),
        );

        $stream->feed(rows(row(int_entry('id', 1))));
        $stream->feed(rows(row(int_entry('id', 2))));

        static::assertSame(1, $sink->loadsCount);

        $stream->drain();

        static::assertSame(1, $sink->loadsCount);
    }

    public function test_a_triggering_transformation_is_refused(): void
    {
        try {
            new TransformationStream(
                new CallbackTransformation(static function (DataFrame $df): DataFrame {
                    $df->count();

                    return $df;
                }),
                new SpyLoader(),
                flow_context(config()),
            );

            static::fail('Expected a Transformation triggering the nested frame to be refused.');
        } catch (InvalidLogicException $e) {
            static::assertStringContainsString('to_branch()->withTransformation()', $e->getMessage());
            static::assertInstanceOf(FiberError::class, $e->getPrevious());
        }
    }

    public function test_drain_before_any_feed_is_a_no_op(): void
    {
        $sink = new SpyLoader();

        (new TransformationStream(select('id'), $sink, flow_context(config())))->drain();

        static::assertSame(0, $sink->loadsCount);
    }

    public function test_drain_flushes_a_blocking_transformation(): void
    {
        $sink = new SpyLoader();
        $stream = new TransformationStream(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect()),
            $sink,
            flow_context(config()),
        );

        $stream->feed(rows(row(int_entry('id', 1))));
        $stream->feed(rows(row(int_entry('id', 2))));
        $stream->feed(rows(row(int_entry('id', 3))));

        $loadsBeforeDrain = $sink->loadsCount;

        $stream->drain();

        static::assertSame(0, $loadsBeforeDrain);
        static::assertSame(1, $sink->loadsCount);
        static::assertSame([3], $sink->loadedRowCounts());
    }

    public function test_feed_delivers_transformed_rows_to_the_sink(): void
    {
        $sink = new SpyLoader();
        $context = flow_context(config());
        $stream = new TransformationStream(select('id'), $sink, $context);

        $stream->feed(rows(row(int_entry('id', 1), int_entry('other', 10))));
        $stream->feed(rows(row(int_entry('id', 2), int_entry('other', 20))));

        static::assertSame(2, $sink->loadsCount);
        static::assertSame([1, 1], $sink->loadedRowCounts());
        static::assertSame([$context, $context], $sink->contexts);
        static::assertSame(
            [[['id' => 1]], [['id' => 2]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $sink->loadedRows),
        );
    }

    public function test_the_drive_knows_the_context_it_was_built_for(): void
    {
        $context = flow_context(config());
        $stream = new TransformationStream(select('id'), new SpyLoader(), $context);

        static::assertTrue($stream->drivenBy($context));
        static::assertFalse($stream->drivenBy(flow_context(config())));
    }
}
