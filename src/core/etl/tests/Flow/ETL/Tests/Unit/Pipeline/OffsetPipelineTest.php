<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\{bool_entry, config, flow_context, from_rows, int_entry, row, rows};
use function Flow\ETL\DSL\{lit, ref};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor, FlowContext};
use Flow\ETL\Pipeline\{OffsetPipeline, Pipes, SynchronousPipeline};
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

    public function test_add_delegates_to_wrapped_pipeline() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows()));
        $offsetPipeline = new OffsetPipeline($pipeline, 1);
        $transformer = new ScalarFunctionTransformer('test', lit('value'));

        $returnedPipeline = $offsetPipeline->add($transformer);

        self::assertSame($offsetPipeline, $returnedPipeline);
        self::assertTrue($pipeline->has(ScalarFunctionTransformer::class));
    }

    public function test_constructor_with_negative_offset_throws_exception() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows()));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0, given: -1');

        // @phpstan-ignore-next-line
        new OffsetPipeline($pipeline, -1);
    }

    public function test_constructor_with_positive_offset() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows()));

        $offsetPipeline = new OffsetPipeline($pipeline, 5);

        self::assertInstanceOf(OffsetPipeline::class, $offsetPipeline);
    }

    public function test_constructor_with_zero_offset() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows()));

        $offsetPipeline = new OffsetPipeline($pipeline, 0);

        self::assertInstanceOf(OffsetPipeline::class, $offsetPipeline);
    }

    public function test_has_delegates_to_wrapped_pipeline() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows()));
        $pipeline->add(new ScalarFunctionTransformer('test', lit('value')));
        $offsetPipeline = new OffsetPipeline($pipeline, 1);

        $result = $offsetPipeline->has(ScalarFunctionTransformer::class);

        self::assertTrue($result);
    }

    public function test_has_returns_false_for_non_existent_transformer() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows()));
        $offsetPipeline = new OffsetPipeline($pipeline, 1);

        $result = $offsetPipeline->has('NonExistentTransformer');

        self::assertFalse($result);
    }

    public function test_pipelines_returns_wrapped_pipeline_in_array() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows()));
        $offsetPipeline = new OffsetPipeline($pipeline, 1);

        $pipelines = $offsetPipeline->pipelines();

        self::assertCount(1, $pipelines);
        self::assertSame($pipeline, $pipelines[0]);
    }

    public function test_pipes_delegates_to_wrapped_pipeline() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows()));
        $transformer = new ScalarFunctionTransformer('test', lit('value'));
        $pipeline->add($transformer);
        $offsetPipeline = new OffsetPipeline($pipeline, 1);

        $pipes = $offsetPipeline->pipes();

        self::assertInstanceOf(Pipes::class, $pipes);
        self::assertTrue($pipes->has(ScalarFunctionTransformer::class));
    }

    public function test_process_maintains_row_structure_with_mixed_entry_types() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(
            rows(
                row(int_entry('id', 1), bool_entry('active', true)),
                row(int_entry('id', 2), bool_entry('active', false)),
                row(int_entry('id', 3), bool_entry('active', true)),
                row(int_entry('id', 4), bool_entry('active', false))
            )
        ));
        $offsetPipeline = new OffsetPipeline($pipeline, 1);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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
        $pipeline = new SynchronousPipeline(from_rows(rows()));
        $offsetPipeline = new OffsetPipeline($pipeline, 5);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

        self::assertCount(0, $result);
    }

    public function test_process_with_multiple_batches_offset_skips_entire_batches() : void
    {
        $pipeline = new SynchronousPipeline(new class implements Extractor {
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
        $offsetPipeline = new OffsetPipeline($pipeline, 4);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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
        $pipeline = new SynchronousPipeline(new class implements Extractor {
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
        $offsetPipeline = new OffsetPipeline($pipeline, 3);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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
        $pipeline = new SynchronousPipeline(new class implements Extractor {
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
        $offsetPipeline = new OffsetPipeline($pipeline, 1);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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
        $pipeline = new SynchronousPipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3))
            )
        ));
        $offsetPipeline = new OffsetPipeline($pipeline, 3);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

        self::assertCount(0, $result);
    }

    public function test_process_with_offset_larger_than_batch_size() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2))
            )
        ));
        $offsetPipeline = new OffsetPipeline($pipeline, 5);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

        self::assertCount(0, $result);
    }

    public function test_process_with_offset_resulting_in_empty_batch() : void
    {
        $pipeline = new SynchronousPipeline(new class implements Extractor {
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
        $offsetPipeline = new OffsetPipeline($pipeline, 2);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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
        $pipeline = new SynchronousPipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4)),
                row(int_entry('id', 5))
            )
        ));
        $offsetPipeline = new OffsetPipeline($pipeline, 2);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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

    public function test_process_with_transformer_applied_to_wrapped_pipeline() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4))
            )
        ));
        $pipeline->add(new ScalarFunctionTransformer('doubled', ref('id')->multiply(lit(2))));
        $offsetPipeline = new OffsetPipeline($pipeline, 1);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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

        $pipeline = new SynchronousPipeline(from_rows(rows(...$rowsData)));
        $offsetPipeline = new OffsetPipeline($pipeline, $offset >= 0 ? $offset : 0);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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
        $pipeline = new SynchronousPipeline(from_rows(
            rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3))
            )
        ));
        $offsetPipeline = new OffsetPipeline($pipeline, 0);

        $result = \iterator_to_array($offsetPipeline->process(flow_context(config())));

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

    public function test_source_delegates_to_wrapped_pipeline() : void
    {
        $extractor = from_rows(rows());
        $pipeline = new SynchronousPipeline($extractor);
        $offsetPipeline = new OffsetPipeline($pipeline, 1);

        $source = $offsetPipeline->source();

        self::assertSame($extractor, $source);
    }
}
