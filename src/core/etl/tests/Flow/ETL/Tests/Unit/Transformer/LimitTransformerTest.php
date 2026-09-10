<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Transformer\LimitTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class LimitTransformerTest extends FlowTestCase
{
    public function test_a_batch_below_the_limit_passes_through(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            (new LimitTransformer(3))
                ->transform(RowsMother::sequentialIds(2), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));

        static::assertEquals($input, (new LimitTransformer(2))->bind($input)->output);
    }

    public function test_exact_fill_signals_on_the_same_batch(): void
    {
        $thrown = null;

        try {
            (new LimitTransformer(3))->transform(RowsMother::sequentialIds(3), flow_context(config()));
        } catch (LimitReachedException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LimitReachedException::class, $thrown);
        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $thrown->rows?->toArray());
    }

    public function test_limit_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: 0");

        new LimitTransformer(0);
    }

    public function test_limit_reached_carries_the_trimmed_batch(): void
    {
        $thrown = null;

        try {
            (new LimitTransformer(3))->transform(RowsMother::sequentialIds(5), flow_context(config()));
        } catch (LimitReachedException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LimitReachedException::class, $thrown);
        static::assertSame(3, $thrown->limit);
        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $thrown->rows?->toArray());
    }

    public function test_limit_spanning_batches_trims_the_batch_that_fills_it(): void
    {
        $limit = new LimitTransformer(3);
        $limit->transform(RowsMother::sequentialIds(2), flow_context(config()));

        $thrown = null;

        try {
            $limit->transform(RowsMother::sequentialIds(2), flow_context(config()));
        } catch (LimitReachedException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LimitReachedException::class, $thrown);
        static::assertSame([['id' => 1]], $thrown->rows?->toArray());
    }
}
