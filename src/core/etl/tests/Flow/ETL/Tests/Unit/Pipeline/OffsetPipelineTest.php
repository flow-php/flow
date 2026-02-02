<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\{bool_entry, config, flow_context, from_rows, int_entry, lit, ref, row, rows};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor, FlowContext, Pipeline};
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use PHPUnit\Framework\Attributes\DataProvider;

final class OffsetPipelineTest extends FlowTestCase
{
    public static function offset_values_data_provider() : \Generator
    {
        yield 'zero offset' => [0];
        yield 'small offset' => [1];
        yield 'medium offset' => [10];
        yield 'large offset' => [100];
    }

    public function test_constructor_with_negative_offset_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0, given: -1');

        // @phpstan-ignore-next-line
        new OffsetProcessor(-1);
    }

    public function test_process_maintains_row_structure_with_mixed_entry_types() : void
    {
        $pipeline = new Pipeline(from_rows(
            rows(
                row(int_entry('id', 1), bool_entry('active', true)),
                row(int_entry('id', 2), bool_entry('active', false)),
                row(int_entry('id', 3), bool_entry('active', true)),
                row(int_entry('id', 4), bool_entry('active', false))
            )
        ));
        $pipeline->add(new OffsetProcessor(1));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(1, $result);
        self::assertCount(3, $result[0]);
        self::assertEquals(
            rows(
                row(int_entry('id', 2), bool_entry('active', false)),
                row(int_entry('id', 3), bool_entry('active', true)),
                row(int_entry('id', 4), bool_entry('active', false))
            ),
            $result[0]
        );
    }

    public function test_process_with_empty_pipeline() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add(new OffsetProcessor(5));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(0, $result);
    }

    public function test_process_with_multiple_batches_offset_skips_entire_batches() : void
    {
        $pipeline = new Pipeline(new class implements Extractor {
            public function extract(FlowContext $context) : \Generator
            {
                yield rows(
                    row(int_entry('id', 1)),
                    row(int_entry('id', 2))
                );
                yield rows(
                    row(int_entry('id', 3)),
                    row(int_entry('id', 4))
                );
                yield rows(
                    row(int_entry('id', 5)),
                    row(int_entry('id', 6))
                );
            }
        });
        $pipeline->add(new OffsetProcessor(4));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(1, $result);
        self::assertEquals(
            rows(
                row(int_entry('id', 5)),
                row(int_entry('id', 6))
            ),
            $result[0]
        );
    }

    public function test_process_with_multiple_batches_offset_spanning_batches() : void
    {
        $pipeline = new Pipeline(new class implements Extractor {
            public function extract(FlowContext $context) : \Generator
            {
                yield rows(
                    row(int_entry('id', 1)),
                    row(int_entry('id', 2))
                );
                yield rows(
                    row(int_entry('id', 3)),
                    row(int_entry('id', 4)),
                    row(int_entry('id', 5))
                );
                yield rows(
                    row(int_entry('id', 6))
                );
            }
        });
        $pipeline->add(new OffsetProcessor(3));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(2, $result);
        self::assertEquals(
            rows(
                row(int_entry('id', 4)),
                row(int_entry('id', 5))
            ),
            $result[0]
        );
        self::assertEquals(
            rows(
                row(int_entry('id', 6))
            ),
            $result[1]
        );
    }

    public function test_process_with_multiple_batches_offset_within_first_batch() : void
    {
        $pipeline = new Pipeline(new class implements Extractor {
            public function extract(FlowContext $context) : \Generator
            {
                yield rows(
                    row(int_entry('id', 1)),
                    row(int_entry('id', 2)),
                    row(int_entry('id', 3))
                );
                yield rows(
                    row(int_entry('id', 4)),
                    row(int_entry('id', 5))
                );
            }
        });
        $pipeline->add(new OffsetProcessor(1));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(2, $result);
        self::assertEquals(
            rows(
                row(int_entry('id', 2)),
                row(int_entry('id', 3))
            ),
            $result[0]
        );
        self::assertEquals(
            rows(
                row(int_entry('id', 4)),
                row(int_entry('id', 5))
            ),
            $result[1]
        );
    }

    public function test_process_with_offset_equal_to_batch_size() : void
    {
        $pipeline = new Pipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3))
            )
        ));
        $pipeline->add(new OffsetProcessor(3));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(0, $result);
    }

    public function test_process_with_offset_larger_than_batch_size() : void
    {
        $pipeline = new Pipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2))
            )
        ));
        $pipeline->add(new OffsetProcessor(5));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(0, $result);
    }

    public function test_process_with_offset_resulting_in_empty_batch() : void
    {
        $pipeline = new Pipeline(new class implements Extractor {
            public function extract(FlowContext $context) : \Generator
            {
                yield rows(
                    row(int_entry('id', 1)),
                    row(int_entry('id', 2))
                );
                yield rows();
                yield rows(
                    row(int_entry('id', 3))
                );
            }
        });
        $pipeline->add(new OffsetProcessor(2));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(1, $result);
        self::assertEquals(
            rows(
                row(int_entry('id', 3))
            ),
            $result[0]
        );
    }

    public function test_process_with_offset_smaller_than_batch_size() : void
    {
        $pipeline = new Pipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4)),
                row(int_entry('id', 5))
            )
        ));
        $pipeline->add(new OffsetProcessor(2));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(1, $result);
        self::assertCount(3, $result[0]);
        self::assertEquals(
            rows(
                row(int_entry('id', 3)),
                row(int_entry('id', 4)),
                row(int_entry('id', 5))
            ),
            $result[0]
        );
    }

    public function test_process_with_transformer_before_offset() : void
    {
        $pipeline = new Pipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4))
            )
        ));
        $pipeline->add(new ScalarFunctionTransformer('doubled', ref('id')->multiply(lit(2))));
        $pipeline->add(new OffsetProcessor(1));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(1, $result);
        self::assertCount(3, $result[0]);

        $rows = $result[0];
        self::assertEquals(2, $rows->all()[0]->valueOf('id'));
        self::assertEquals(4, $rows->all()[0]->valueOf('doubled'));
        self::assertEquals(3, $rows->all()[1]->valueOf('id'));
        self::assertEquals(6, $rows->all()[1]->valueOf('doubled'));
    }

    #[DataProvider('offset_values_data_provider')]
    public function test_process_with_various_offset_values(int $offset) : void
    {
        $rowsData = [];

        for ($i = 1; $i <= 20; $i++) {
            $rowsData[] = row(int_entry('id', $i));
        }

        $pipeline = new Pipeline(from_rows(rows(...$rowsData)));
        $pipeline->add(new OffsetProcessor($offset >= 0 ? $offset : 0));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        $expectedCount = \max(0, 20 - $offset);
        $totalRows = \array_sum(\array_map(fn ($batch) => $batch->count(), $result));

        self::assertEquals($expectedCount, $totalRows);

        if ($expectedCount > 0) {
            $firstRowId = $result[0]->first()->valueOf('id');
            self::assertEquals($offset + 1, $firstRowId);
        }
    }

    public function test_process_with_zero_offset_returns_all_data() : void
    {
        $pipeline = new Pipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3))
            )
        ));
        $pipeline->add(new OffsetProcessor(0));

        $result = \iterator_to_array($pipeline->process(flow_context(config())));

        self::assertCount(1, $result);
        self::assertCount(3, $result[0]);
        self::assertEquals(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3))
            ),
            $result[0]
        );
    }
}
