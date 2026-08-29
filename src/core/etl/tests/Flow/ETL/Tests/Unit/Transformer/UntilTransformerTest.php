<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\UntilTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class UntilTransformerTest extends FlowTestCase
{
    public function test_passes_rows_until_the_predicate_stops_holding(): void
    {
        static::assertSame(
            [
                ['id' => 1],
                ['id' => 2],
            ],
            (new UntilTransformer(ref('id')->lessThan(lit(3))))
                ->transform(
                    rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))),
                    flow_context(config()),
                )
                ->toArray(),
        );
    }

    public function test_a_reached_limit_stops_the_next_batch(): void
    {
        $transformer = new UntilTransformer(ref('id')->lessThan(lit(1)));
        $transformer->transform(rows(row(int_entry('id', 5))), flow_context(config()));

        $this->expectException(LimitReachedException::class);

        $transformer->transform(rows(row(int_entry('id', 0))), flow_context(config()));
    }

    public function test_a_non_boolean_predicate_is_rejected_at_bind(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('until() requires a predicate returning boolean');

        (new UntilTransformer(ref('id')))->transform(rows(row(int_entry('id', 1))), flow_context(config()));
    }

    public function test_an_empty_batch_is_returned_without_binding(): void
    {
        $empty = rows();

        static::assertSame($empty, (new UntilTransformer(ref('missing')->equals(lit(1))))->transform(
            $empty,
            flow_context(config()),
        ));
    }

    public function test_a_reached_limit_stops_an_empty_next_batch(): void
    {
        $transformer = new UntilTransformer(ref('id')->lessThan(lit(1)));
        $transformer->transform(rows(row(int_entry('id', 5))), flow_context(config()));

        $this->expectException(LimitReachedException::class);

        $transformer->transform(rows(), flow_context(config()));
    }
}
