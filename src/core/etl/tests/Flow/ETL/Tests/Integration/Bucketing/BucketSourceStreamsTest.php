<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Bucketing;

use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Memory\Unit;
use Flow\Telemetry\Tracer\Span;

use function array_filter;
use function count;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_array;

final class BucketSourceStreamsTest extends FlowIntegrationTestCase
{
    public function test_bucket_source_streams_are_not_nested_into_each_other(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(collect_metrics: false));

        $source = [];

        for ($id = 1; $id <= 200; $id++) {
            $source[] = ['group_id' => 'group-' . $id, 'value' => $id];
        }

        $output = [];
        df($context->config)
            ->read(from_array($source))
            ->batchSize(10)
            ->groupBy([ref(
                'group_id',
            )], hash_group_by()->storage($context->traceableStorage($this->cacheDir))->memoryLimit(Unit::fromBytes(1))->batchSize(10)->bucketsCount(4))
            ->aggregate(collect(ref('value')->as('values')))
            ->write(to_array($output))
            ->run();

        $isFilesystemSpan = static fn(Span $span): bool => (
            $span->name() === 'filesystem.read'
            || $span->name() === 'filesystem.write'
        );

        $filesystemSpanIds = [];

        foreach (array_filter($context->spans->endedSpans(), $isFilesystemSpan) as $span) {
            $filesystemSpanIds[(string) $span->context()->spanId] = true;
        }

        static::assertNotSame([], $filesystemSpanIds);

        foreach (array_filter($context->spans->endedSpans(), $isFilesystemSpan) as $span) {
            $parentSpanId = $span->context()->parentSpanId;

            static::assertArrayNotHasKey(
                $parentSpanId === null ? '' : (string) $parentSpanId,
                $filesystemSpanIds,
                'A filesystem span must never be the parent of another filesystem span.',
            );
        }
    }

    public function test_external_sort_closes_every_bucket_source_stream(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(collect_metrics: false));

        $source = [];

        for ($id = 1; $id <= 500; $id++) {
            $source[] = ['id' => (1_000 - $id) % 337];
        }

        $output = [];
        df($context->config)
            ->read(from_array($source))
            ->sortBy([ref(
                'id',
            )], external_sort()->storage($context->traceableStorage($this->cacheDir))->memoryLimit(Unit::fromBytes(1))->batchSize(50)->bucketsCount(8))
            ->write(to_array($output))
            ->run();

        $isRead = static fn(Span $span): bool => $span->name() === 'filesystem.read';

        static::assertNotSame(0, count(array_filter($context->spans->startedSpans(), $isRead)));
        static::assertSame(
            count(array_filter($context->spans->startedSpans(), $isRead)),
            count(array_filter($context->spans->endedSpans(), $isRead)),
        );
    }

    public function test_group_by_closes_every_bucket_source_stream(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(collect_metrics: false));

        $source = [];

        for ($id = 1; $id <= 200; $id++) {
            $source[] = ['group_id' => 'group-' . $id, 'value' => $id];
        }

        $output = [];
        df($context->config)
            ->read(from_array($source))
            ->batchSize(10)
            ->groupBy([ref(
                'group_id',
            )], hash_group_by()->storage($context->traceableStorage($this->cacheDir))->memoryLimit(Unit::fromBytes(1))->batchSize(10)->bucketsCount(4))
            ->aggregate(sum(ref('value')->as('total')))
            ->write(to_array($output))
            ->run();

        $isRead = static fn(Span $span): bool => $span->name() === 'filesystem.read';

        static::assertNotSame(0, count(array_filter($context->spans->startedSpans(), $isRead)));
        static::assertSame(
            count(array_filter($context->spans->startedSpans(), $isRead)),
            count(array_filter($context->spans->endedSpans(), $isRead)),
        );
    }
}
