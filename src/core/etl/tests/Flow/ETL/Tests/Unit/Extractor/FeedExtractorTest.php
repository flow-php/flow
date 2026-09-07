<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Fiber;
use FiberError;
use Flow\ETL\Extractor\FeedExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class FeedExtractorTest extends FlowTestCase
{
    public function test_extract_outside_a_fiber_fails(): void
    {
        $generator = (new FeedExtractor(schema(int_schema('id'))))->extract(flow_context(config()));

        $this->expectException(FiberError::class);

        $generator->current();
    }

    public function test_finish_ends_the_stream(): void
    {
        $extractor = new FeedExtractor(schema(int_schema('id')));
        $context = flow_context(config());
        $collected = [];
        $completed = false;

        $extractor->feed(rows(schema(int_schema('id')), row(['id' => 1])));

        $fiber = new Fiber(static function () use ($extractor, $context, &$collected, &$completed): void {
            foreach ($extractor->extract($context) as $rows) {
                $collected[] = $rows;
            }

            $completed = true;
        });

        $fiber->start();
        $extractor->finish();
        $fiber->resume();

        static::assertTrue($fiber->isTerminated());
        static::assertTrue($completed);
        static::assertSame([1, 0], array_map(static fn(Rows $rows): int => $rows->count(), $collected));
    }

    public function test_it_describes_the_shape_it_was_seeded_with(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            (new FeedExtractor(schema(int_schema('id'), str_schema('name'))))->schema(),
        );
    }

    public function test_resumes_with_the_next_fed_batch(): void
    {
        $extractor = new FeedExtractor(schema(int_schema('id')));
        $context = flow_context(config());
        $collected = [];

        $extractor->feed(rows(schema(int_schema('id')), row(['id' => 1])));

        $fiber = new Fiber(static function () use ($extractor, $context, &$collected): void {
            foreach ($extractor->extract($context) as $rows) {
                $collected[] = $rows;
            }
        });

        $fiber->start();
        $extractor->feed(rows(schema(int_schema('id')), row(['id' => 2])));
        $fiber->resume();

        static::assertTrue($fiber->isSuspended());
        static::assertSame(
            [[['id' => 1]], [], [['id' => 2]], []],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $collected),
        );
    }

    public function test_stop_signal_at_a_batch_yield_ends_the_stream(): void
    {
        $extractor = new FeedExtractor(schema(int_schema('id')));
        $context = flow_context(config());
        $batch = null;
        $valid = null;

        $extractor->feed(rows(schema(int_schema('id')), row(['id' => 1])));

        $fiber = new Fiber(static function () use ($extractor, $context, &$batch, &$valid): void {
            $generator = $extractor->extract($context);
            $batch = $generator->current();
            $generator->send(Signal::STOP);
            $valid = $generator->valid();
        });

        $fiber->start();

        static::assertTrue($fiber->isTerminated());
        static::assertSame([['id' => 1]], $batch->toArray());
        static::assertFalse($valid);
    }

    public function test_stop_signal_at_the_trailing_yield_ends_the_stream(): void
    {
        $extractor = new FeedExtractor(schema(int_schema('id')));
        $context = flow_context(config());
        $trailing = null;
        $valid = null;

        $extractor->feed(rows(schema(int_schema('id')), row(['id' => 1])));

        $fiber = new Fiber(static function () use ($extractor, $context, &$trailing, &$valid): void {
            $generator = $extractor->extract($context);
            $generator->next();
            $trailing = $generator->current();
            $generator->send(Signal::STOP);
            $valid = $generator->valid();
        });

        $fiber->start();

        static::assertTrue($fiber->isTerminated());
        static::assertSame([], $trailing->toArray());
        static::assertFalse($valid);
    }

    public function test_with_schema_replaces_the_seeded_shape(): void
    {
        static::assertEquals(
            schema(str_schema('name')),
            (new FeedExtractor(schema(int_schema('id'))))
                ->withSchema(schema(str_schema('name')))
                ->schema(),
        );
    }

    public function test_yields_the_fed_batch_and_a_trailing_empty_rows_then_suspends(): void
    {
        $extractor = new FeedExtractor(schema(int_schema('id')));
        $context = flow_context(config());
        $collected = [];

        $extractor->feed(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])));

        $fiber = new Fiber(static function () use ($extractor, $context, &$collected): void {
            foreach ($extractor->extract($context) as $rows) {
                $collected[] = $rows;
            }
        });

        $fiber->start();

        static::assertTrue($fiber->isSuspended());
        static::assertSame(
            [[['id' => 1], ['id' => 2]], []],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $collected),
        );
    }
}
