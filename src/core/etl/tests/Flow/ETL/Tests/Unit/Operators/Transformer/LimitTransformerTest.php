<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
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
                ->transform(RowsMother::sequentialIds(2), flow_context(config())->backend())
                ->toArray(),
        );
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));

        static::assertEquals($input, (new LimitTransformer(2))->bind($input)->output);
    }

    public function test_a_batch_below_the_limit_does_not_reach_it(): void
    {
        $limit = new LimitTransformer(3);
        $limit->transform(RowsMother::sequentialIds(2), flow_context(config())->backend());

        static::assertFalse($limit->reached());
    }

    public function test_exact_fill_reaches_the_limit_on_the_same_batch(): void
    {
        $limit = new LimitTransformer(3);

        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3]],
            $limit->transform(RowsMother::sequentialIds(3), flow_context(config())->backend())->toArray(),
        );
        static::assertTrue($limit->reached());
    }

    public function test_limit_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: 0");

        new LimitTransformer(0);
    }

    public function test_reaching_the_limit_trims_the_batch(): void
    {
        $limit = new LimitTransformer(3);

        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3]],
            $limit->transform(RowsMother::sequentialIds(5), flow_context(config())->backend())->toArray(),
        );
        static::assertTrue($limit->reached());
    }

    public function test_limit_spanning_batches_trims_the_batch_that_fills_it(): void
    {
        $limit = new LimitTransformer(3);
        $limit->transform(RowsMother::sequentialIds(2), flow_context(config())->backend());

        static::assertSame(
            [['id' => 1]],
            $limit->transform(RowsMother::sequentialIds(2), flow_context(config())->backend())->toArray(),
        );
        static::assertTrue($limit->reached());
    }

    public function test_a_batch_after_the_limit_is_reached_is_dropped_whole(): void
    {
        $limit = new LimitTransformer(2);
        $limit->transform(RowsMother::sequentialIds(2), flow_context(config())->backend());

        static::assertSame(
            [],
            $limit->transform(RowsMother::sequentialIds(3), flow_context(config())->backend())->toArray(),
        );
        static::assertTrue($limit->reached());
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new LimitTransformer(5);
        $rebuilt = DescribeRoundTrip::rebuilt($transformer);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertSame(
            StepRoundTrip::transformed($transformer, $schema, $rows, flow_context()),
            StepRoundTrip::transformed($rebuilt, $schema, $rows, flow_context()),
        );
    }

    public function test_a_rebuilt_limit_counts_from_zero(): void
    {
        $limit = new LimitTransformer(5);
        $rows = RowsMother::sequentialIds(3);
        $limit->transform($rows, flow_context(config())->backend());
        $rebuilt = DescribeRoundTrip::rebuilt($limit);

        static::assertCount(3, $rebuilt->transform($rows, flow_context(config())->backend())->toArray());
        static::assertFalse($rebuilt->reached());
        static::assertCount(2, $limit->transform($rows, flow_context(config())->backend())->toArray());
        static::assertTrue($limit->reached());
    }
}
