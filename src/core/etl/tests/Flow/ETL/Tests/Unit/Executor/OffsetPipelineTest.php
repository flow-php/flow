<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Executor\Segments;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Context\ExecutedSegments;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_sum;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
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
        $segments = new Segments(from_rows(array_to_rows(
            [
                ['id' => 1, 'active' => true],
                ['id' => 2, 'active' => false],
                ['id' => 3, 'active' => true],
                ['id' => 4, 'active' => false],
            ],
            schema(int_schema('id'), bool_schema('active')),
        )));
        $segments->add(new OffsetProcessor(1));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);
        static::assertEquals(
            array_to_rows(
                [['id' => 2, 'active' => false], ['id' => 3, 'active' => true], ['id' => 4, 'active' => false]],
                schema(int_schema('id'), bool_schema('active')),
            ),
            $result[0],
        );
    }

    public function test_process_with_empty_pipeline(): void
    {
        $segments = new Segments(from_rows(rows(schema())));
        $segments->add(new OffsetProcessor(5));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(0, $result);
    }

    public function test_process_with_multiple_batches_offset_skips_entire_batches(): void
    {
        $segments = new Segments(new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return new Schema();
            }

            public function extract(FlowContext $context, ?int $limit = null): Generator
            {
                yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                yield array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id')));
                yield array_to_rows([['id' => 5], ['id' => 6]], schema(int_schema('id')));
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }
        });
        $segments->add(new OffsetProcessor(4));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(1, $result);
        static::assertEquals(array_to_rows([['id' => 5], ['id' => 6]], schema(int_schema('id'))), $result[0]);
    }

    public function test_process_with_multiple_batches_offset_spanning_batches(): void
    {
        $segments = new Segments(new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return new Schema();
            }

            public function extract(FlowContext $context, ?int $limit = null): Generator
            {
                yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                yield array_to_rows([['id' => 3], ['id' => 4], ['id' => 5]], schema(int_schema('id')));
                yield array_to_rows([['id' => 6]], schema(int_schema('id')));
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }
        });
        $segments->add(new OffsetProcessor(3));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(2, $result);
        static::assertEquals(array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id'))), $result[0]);
        static::assertEquals(array_to_rows([['id' => 6]], schema(int_schema('id'))), $result[1]);
    }

    public function test_process_with_multiple_batches_offset_within_first_batch(): void
    {
        $segments = new Segments(new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return new Schema();
            }

            public function extract(FlowContext $context, ?int $limit = null): Generator
            {
                yield array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
                yield array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id')));
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }
        });
        $segments->add(new OffsetProcessor(1));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(2, $result);
        static::assertEquals(array_to_rows([['id' => 2], ['id' => 3]], schema(int_schema('id'))), $result[0]);
        static::assertEquals(array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id'))), $result[1]);
    }

    public function test_process_with_offset_equal_to_batch_size(): void
    {
        $segments = new Segments(from_rows(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id')))));
        $segments->add(new OffsetProcessor(3));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(0, $result);
    }

    public function test_process_with_offset_larger_than_batch_size(): void
    {
        $segments = new Segments(from_rows(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')))));
        $segments->add(new OffsetProcessor(5));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(0, $result);
    }

    public function test_process_with_offset_resulting_in_empty_batch(): void
    {
        $segments = new Segments(new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return new Schema();
            }

            public function extract(FlowContext $context, ?int $limit = null): Generator
            {
                yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                yield rows(schema());
                yield array_to_rows([['id' => 3]], schema(int_schema('id')));
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }
        });
        $segments->add(new OffsetProcessor(2));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(1, $result);
        static::assertEquals(array_to_rows([['id' => 3]], schema(int_schema('id'))), $result[0]);
    }

    public function test_process_with_offset_smaller_than_batch_size(): void
    {
        $segments = new Segments(from_rows(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
        ], schema(int_schema('id')))));
        $segments->add(new OffsetProcessor(2));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);
        static::assertEquals(
            array_to_rows([['id' => 3], ['id' => 4], ['id' => 5]], schema(int_schema('id'))),
            $result[0],
        );
    }

    public function test_process_with_transformer_before_offset(): void
    {
        $segments = new Segments(from_rows(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
        ], schema(int_schema('id')))));
        $segments->add(new ScalarFunctionTransformer('doubled', ref('id')->multiply(lit(2))));
        $segments->add(new OffsetProcessor(1));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);
        $rows = $result[0];
        static::assertEquals(2, $rows->column('id')->value(0));
        static::assertEquals(4, $rows->column('doubled')->value(0));
        static::assertEquals(3, $rows->column('id')->value(1));
        static::assertEquals(6, $rows->column('doubled')->value(1));
    }

    #[DataProvider('offset_values_data_provider')]
    public function test_process_with_various_offset_values(int $offset): void
    {
        $rowsData = [];
        for ($i = 1; $i <= 20; $i++) {
            $rowsData[] = ['id' => $i];
        }
        $segments = new Segments(from_rows(array_to_rows($rowsData, schema(int_schema('id')))));
        $segments->add(new OffsetProcessor($offset >= 0 ? $offset : 0));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        $expectedCount = max(0, 20 - $offset);
        $totalRows = array_sum(array_map(static fn($batch) => $batch->count(), $result));
        static::assertEquals($expectedCount, $totalRows);
        if ($expectedCount > 0) {
            // @mago-ignore analysis:mixed-assignment
            $firstRowId = $result[0]->column('id')->value(0);
            static::assertEquals($offset + 1, $firstRowId);
        }
    }

    public function test_process_with_zero_offset_returns_all_data(): void
    {
        $segments = new Segments(from_rows(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id')))));
        $segments->add(new OffsetProcessor(0));
        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));
        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);
        static::assertEquals(
            array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))),
            $result[0],
        );
    }
}
