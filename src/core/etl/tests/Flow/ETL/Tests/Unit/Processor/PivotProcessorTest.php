<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor\PivotProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\collect_unique;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_agg;
use function Flow\ETL\DSL\sum;

final class PivotProcessorTest extends FlowTestCase
{
    public function test_bind_derives_the_group_by_columns_and_one_nullable_column_per_pivot_value(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), pivot_values('norbert', 'stloyd'));
        $groupBy->aggregate(sum(ref('contributions')));

        static::assertEquals(
            schema(str_schema('date'), float_schema('norbert', nullable: true), float_schema('stloyd', nullable: true)),
            (new PivotProcessor($groupBy))->bind(schema(
                str_schema('date'),
                str_schema('user'),
                int_schema('contributions'),
            ))->output,
        );
    }

    public function test_bind_refuses_a_pivot_value_colliding_with_a_group_by_column(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), pivot_values('date'));
        $groupBy->aggregate(sum(ref('contributions')));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pivot value "date" collides with the group-by column of the same name');

        (new PivotProcessor($groupBy))->bind(schema(
            str_schema('date'),
            str_schema('user'),
            int_schema('contributions'),
        ));
    }

    public function test_bind_declares_the_pivot_columns_with_the_resolved_aggregate_type(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), pivot_values('norbert', 'stloyd'));
        $groupBy->aggregate(min(ref('contributions')));

        static::assertEquals(
            schema(str_schema('date'), int_schema('norbert', nullable: true), int_schema('stloyd', nullable: true)),
            (new PivotProcessor($groupBy))->bind(schema(
                str_schema('date'),
                str_schema('user'),
                int_schema('contributions'),
            ))->output,
        );
    }

    /**
     * @return Generator<string, array{AggregatingFunction, string}>
     */
    public static function pivotAggregates(): Generator
    {
        yield 'min' => [min(ref('contributions')), 'integer'];
        yield 'max' => [max(ref('contributions')), 'integer'];
        yield 'first' => [first(ref('contributions')), 'integer'];
        yield 'last' => [last(ref('contributions')), 'integer'];
        yield 'collect' => [collect(ref('contributions')), 'list<integer>'];
        yield 'collect_unique' => [collect_unique(ref('contributions')), 'list<integer>'];
        yield 'sum' => [sum(ref('contributions')), 'float'];
        yield 'count' => [count(ref('contributions')), 'integer'];
        yield 'average' => [average(ref('contributions')), 'float'];
        yield 'string_agg' => [string_agg(ref('note')), 'string'];
    }

    #[DataProvider('pivotAggregates')]
    public function test_bind_resolves_every_pivot_aggregate(AggregatingFunction $aggregate, string $resolvedType): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), pivot_values('norbert', 'stloyd'));
        $groupBy->aggregate($aggregate);

        $output = (new PivotProcessor($groupBy))->bind(schema(
            str_schema('date'),
            str_schema('user'),
            int_schema('contributions'),
            str_schema('note'),
        ))->output;

        static::assertSame(['date', 'norbert', 'stloyd'], $output->references()->names());

        foreach (['norbert', 'stloyd'] as $column) {
            static::assertSame($resolvedType, $output->get($column)->type()->toString());
            static::assertTrue($output->get($column)->isNullable());
        }
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new PivotProcessor(new GroupBy(ref('date')), 0);
    }

    public function test_a_bound_processor_pivots_grouped_rows(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), pivot_values('norbert', 'stloyd'));
        $groupBy->aggregate(sum(ref('contributions')));

        $input = schema(str_schema('date'), str_schema('user'), int_schema('contributions'));
        $bound = (new PivotProcessor($groupBy))->bind($input);

        static::assertInstanceOf(PivotProcessor::class, $bound->step);

        $result = iterator_to_array(
            $bound->step->process(
                (static function () use ($input): Generator {
                    yield rows(
                        $input,
                        row(['date' => '2024-01-01', 'user' => 'norbert', 'contributions' => 2]),
                        row(['date' => '2024-01-01', 'user' => 'stloyd', 'contributions' => 3]),
                        row(['date' => '2024-01-02', 'user' => 'norbert', 'contributions' => 5]),
                    );
                })(),
                flow_context(),
            ),
            preserve_keys: false,
        );

        static::assertCount(1, $result);
        static::assertSame(
            [
                ['date' => '2024-01-01', 'norbert' => 2.0, 'stloyd' => 3.0],
                ['date' => '2024-01-02', 'norbert' => 5.0, 'stloyd' => null],
            ],
            $result[0]->toArray(),
        );
        static::assertEquals($bound->output, $result[0]->schema());
    }

    public function test_pivots_grouped_rows(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), pivot_values('norbert', 'stloyd'));
        $groupBy->aggregate(sum(ref('contributions')));

        $input = (static function (): Generator {
            yield rows(
                schema(str_schema('date'), str_schema('user'), int_schema('contributions')),
                row(['date' => '2024-01-01', 'user' => 'norbert', 'contributions' => 2]),
                row(['date' => '2024-01-01', 'user' => 'stloyd', 'contributions' => 3]),
                row(['date' => '2024-01-02', 'user' => 'norbert', 'contributions' => 5]),
            );
        })();

        $result = iterator_to_array(
            (new PivotProcessor($groupBy))->process($input, flow_context()),
            preserve_keys: false,
        );

        $pivoted = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $pivotRow) {
                $pivoted[$pivotRow['date']] = $pivotRow;
            }
        }

        static::assertSame(
            [
                '2024-01-01' => ['date' => '2024-01-01', 'norbert' => 2.0, 'stloyd' => 3.0],
                '2024-01-02' => ['date' => '2024-01-02', 'norbert' => 5.0, 'stloyd' => null],
            ],
            $pivoted,
        );
    }
}
