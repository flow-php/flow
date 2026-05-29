<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_sum;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function iterator_to_array;
use function max;

final class OffsetPipelineTest extends FlowTestCase
{
    public static function offset_values_data_provider(): Generator
    {
        yield 'zero offset' => [0];
        yield 'small offset' => [1];
        yield 'medium offset' => [10];
        yield 'large offset' => [100];
    }

    public function test_constructor_with_negative_offset_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0, given: -1');
        // @mago-ignore analysis:invalid-argument
        new OffsetProcessor(-1);
    }

    public function test_process_maintains_row_structure_with_mixed_entry_types(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(int_entry('id', 1), bool_entry('active', true)),
            row(int_entry('id', 2), bool_entry('active', false)),
            row(int_entry('id', 3), bool_entry('active', true)),
            row(int_entry('id', 4), bool_entry('active', false)),
        )));
        $pipeline->add(new OffsetProcessor(1));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);
        static::assertEquals(
            rows(
                row(int_entry('id', 2), bool_entry('active', false)),
                row(int_entry('id', 3), bool_entry('active', true)),
                row(int_entry('id', 4), bool_entry('active', false)),
            ),
            $result[0],
        );
    }

    public function test_process_with_empty_pipeline(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add(new OffsetProcessor(5));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(0, $result);
    }

    public function test_process_with_multiple_batches_offset_skips_entire_batches(): void
    {
        $pipeline = new Pipeline(new class implements Extractor {
            public function extract(FlowContext $context): Generator
            {
                yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
                yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)));
                yield rows(row(int_entry('id', 5)), row(int_entry('id', 6)));
            }
        });
        $pipeline->add(new OffsetProcessor(4));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(1, $result);
        static::assertEquals(rows(row(int_entry('id', 5)), row(int_entry('id', 6))), $result[0]);
    }

    public function test_process_with_multiple_batches_offset_spanning_batches(): void
    {
        $pipeline = new Pipeline(new class implements Extractor {
            public function extract(FlowContext $context): Generator
            {
                yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
                yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)), row(int_entry('id', 5)));
                yield rows(row(int_entry('id', 6)));
            }
        });
        $pipeline->add(new OffsetProcessor(3));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(2, $result);
        static::assertEquals(rows(row(int_entry('id', 4)), row(int_entry('id', 5))), $result[0]);
        static::assertEquals(rows(row(int_entry('id', 6))), $result[1]);
    }

    public function test_process_with_multiple_batches_offset_within_first_batch(): void
    {
        $pipeline = new Pipeline(new class implements Extractor {
            public function extract(FlowContext $context): Generator
            {
                yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3)));
                yield rows(row(int_entry('id', 4)), row(int_entry('id', 5)));
            }
        });
        $pipeline->add(new OffsetProcessor(1));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(2, $result);
        static::assertEquals(rows(row(int_entry('id', 2)), row(int_entry('id', 3))), $result[0]);
        static::assertEquals(rows(row(int_entry('id', 4)), row(int_entry('id', 5))), $result[1]);
    }

    public function test_process_with_offset_equal_to_batch_size(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        )));
        $pipeline->add(new OffsetProcessor(3));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(0, $result);
    }

    public function test_process_with_offset_larger_than_batch_size(): void
    {
        $pipeline = new Pipeline(from_rows(rows(row(int_entry('id', 1)), row(int_entry('id', 2)))));
        $pipeline->add(new OffsetProcessor(5));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(0, $result);
    }

    public function test_process_with_offset_resulting_in_empty_batch(): void
    {
        $pipeline = new Pipeline(new class implements Extractor {
            public function extract(FlowContext $context): Generator
            {
                yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
                yield rows();
                yield rows(row(int_entry('id', 3)));
            }
        });
        $pipeline->add(new OffsetProcessor(2));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(1, $result);
        static::assertEquals(rows(row(int_entry('id', 3))), $result[0]);
    }

    public function test_process_with_offset_smaller_than_batch_size(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
            row(int_entry('id', 4)),
            row(int_entry('id', 5)),
        )));
        $pipeline->add(new OffsetProcessor(2));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);
        static::assertEquals(
            rows(row(int_entry('id', 3)), row(int_entry('id', 4)), row(int_entry('id', 5))),
            $result[0],
        );
    }

    public function test_process_with_transformer_before_offset(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
            row(int_entry('id', 4)),
        )));
        $pipeline->add(new ScalarFunctionTransformer('doubled', ref('id')->multiply(lit(2))));
        $pipeline->add(new OffsetProcessor(1));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);
        $rows = $result[0];
        static::assertEquals(2, $rows->all()[0]->valueOf('id'));
        static::assertEquals(4, $rows->all()[0]->valueOf('doubled'));
        static::assertEquals(3, $rows->all()[1]->valueOf('id'));
        static::assertEquals(6, $rows->all()[1]->valueOf('doubled'));
    }

    #[DataProvider('offset_values_data_provider')]
    public function test_process_with_various_offset_values(int $offset): void
    {
        $rowsData = [];
        for ($i = 1; $i <= 20; $i++) {
            $rowsData[] = row(int_entry('id', $i));
        }
        $pipeline = new Pipeline(from_rows(rows(...$rowsData)));
        $pipeline->add(new OffsetProcessor($offset >= 0 ? $offset : 0));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        $expectedCount = max(0, 20 - $offset);
        $totalRows = array_sum(array_map(static fn($batch) => $batch->count(), $result));
        static::assertEquals($expectedCount, $totalRows);
        if ($expectedCount > 0) {
            $firstRowId = $result[0]->first()->valueOf('id');
            static::assertEquals($offset + 1, $firstRowId);
        }
    }

    public function test_process_with_zero_offset_returns_all_data(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        )));
        $pipeline->add(new OffsetProcessor(0));
        $result = iterator_to_array($pipeline->process(flow_context(config())));
        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);
        static::assertEquals(
            rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))),
            $result[0],
        );
    }
}
