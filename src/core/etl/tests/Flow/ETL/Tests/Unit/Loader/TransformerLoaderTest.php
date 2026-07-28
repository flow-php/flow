<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DataFrame;
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\Telemetry\Tracer\Span;

use function array_filter;
use function array_map;
use function count;
use function Flow\ETL\DSL\add_row_index;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Types\DSL\type_string;

final class TransformerLoaderTest extends FlowTestCase
{
    public function test_limit_reached_is_reported_once_per_loader(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $loader = to_transformation(new LimitTransformer(1), to_memory(new ArrayMemory()));
        $batch = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));

        $loader->load($batch, $context->flowContext);
        $loader->load($batch, $context->flowContext);
        $loader->load($batch, $context->flowContext);

        static::assertCount(1, $context->logs->entriesContaining('Limit reached'));
        static::assertEmpty($context->logs->entriesContaining('Loading failed'));

        $endedSpans = $context->spans->endedSpans();

        // MemoryLoader runs only on the first call; the other two throw before reaching it.
        static::assertSame(
            ['MemoryLoader', 'TransformerLoader', 'TransformerLoader', 'TransformerLoader'],
            array_map(static fn(Span $span): string => $span->name(), $endedSpans),
        );

        foreach ($endedSpans as $span) {
            static::assertNull($span->status());
        }
    }

    public function test_closure_rebuilds_the_transformation_against_the_next_context(): void
    {
        $first = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $second = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $loader = to_transformation(select('id'), to_memory(new ArrayMemory()));

        $loader->load(rows(row(int_entry('id', 1))), $first->flowContext);
        $loader->closure($first->flowContext);
        $loader->load(rows(row(int_entry('id', 2))), $second->flowContext);

        $memoryLoaderSpans = static fn(MemoryTelemetryContext $context): int => count(array_filter(
            $context->spans->endedSpans(),
            static fn(Span $span): bool => $span->name() === 'MemoryLoader',
        ));

        static::assertSame(1, $memoryLoaderSpans($first));
        static::assertSame(1, $memoryLoaderSpans($second));
    }

    public function test_closure_resets_stateful_transformation_state(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(add_row_index('n', StartFrom::ONE), $spy);

        $loader->load(rows(row(int_entry('id', 1))), $context);
        $loader->load(rows(row(int_entry('id', 2))), $context);
        $loader->closure($context);
        $loader->load(rows(row(int_entry('id', 3))), $context);

        static::assertSame(
            [[['id' => 1, 'n' => 1]], [['id' => 2, 'n' => 2]], [['id' => 3, 'n' => 1]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_stateful_transformation_keeps_state_across_batches(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(add_row_index('n', StartFrom::ONE), $spy);

        for ($id = 1; $id <= 3; $id++) {
            $loader->load(rows(row(int_entry('id', $id))), $context);
        }

        static::assertSame(3, $spy->loadsCount);
        static::assertSame(
            [[['id' => 1, 'n' => 1]], [['id' => 2, 'n' => 2]], [['id' => 3, 'n' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_stateless_transformation_applies_to_every_batch(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(select('id'), $spy);

        for ($id = 1; $id <= 3; $id++) {
            $loader->load(rows(row(int_entry('id', $id), str_entry('name', 'name-' . $id))), $context);
        }

        static::assertSame(3, $spy->loadsCount);
        static::assertSame(
            [[['id' => 1]], [['id' => 2]], [['id' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_transformer_loader(): void
    {
        $transformerMock = $this->createMock(Transformer::class);
        $transformerMock->expects(self::once())->method('transform')->willReturn(rows());

        $loaderMock = $this->createMock(Loader::class);
        $loaderMock->expects(self::once())->method('load');

        $transformer = to_transformation($transformerMock, $loaderMock);

        $transformer->load(rows(), flow_context(config()));
    }

    public function test_transformer_loader_with_transformation(): void
    {
        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]))
            ->write(to_transformation(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->withEntry('id_string', ref('id')->cast(type_string()));
                }
            }, to_memory($memory = new ArrayMemory())))
            ->run();

        static::assertEquals(
            [
                ['id' => 1, 'id_string' => '1'],
                ['id' => 2, 'id_string' => '2'],
                ['id' => 3, 'id_string' => '3'],
            ],
            $memory->dump(),
        );
    }
}
