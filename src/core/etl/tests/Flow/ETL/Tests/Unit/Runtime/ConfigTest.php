<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime;

use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Planning\Optimizer\Rule\CombineLimits;
use Flow\ETL\Planning\Optimizer\Rule\CombineSortAndLimit;
use Flow\ETL\Planning\Optimizer\Rule\CountFromStatistics;
use Flow\ETL\Planning\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Planning\Optimizer\Rule\PushLimitIntoSource;
use Flow\ETL\Planning\Optimizer\Rule\PushOffsetIntoSource;
use Flow\ETL\Planning\Optimizer\Rule\PushProjectionIntoExpand;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\Telemetry\DSL\resource;
use function Flow\Telemetry\DSL\telemetry;

final class ConfigTest extends FlowTestCase
{
    public function test_telemetry_attributes_of_the_default_config(): void
    {
        static::assertSame(
            [
                'cache' => FilesystemRowsStorage::class,
                'serializer' => FloeSerializer::class,
                'optimizer_rules' => [
                    CombineLimits::class,
                    CombineSortAndLimit::class,
                    PushFilterIntoSource::class,
                    PushOffsetIntoSource::class,
                    PushLimitIntoSource::class,
                    CountFromStatistics::class,
                    PushProjectionIntoExpand::class,
                ],
                'telemetry' => [
                    'trace_loading' => false,
                    'trace_transformations' => false,
                    'collect_metrics' => false,
                ],
                'spill' => [
                    'sort' => FilesystemRowsStorage::class,
                    'group_by' => FilesystemRowsStorage::class,
                    'join' => FilesystemRowsStorage::class,
                ],
            ],
            config()->telemetryAttributes(),
        );
    }

    public function test_telemetry_attributes_follow_the_telemetry_options_and_a_memory_sort(): void
    {
        $attributes = config_builder()
            ->sort(memory_sort())
            ->withTelemetry(telemetry(resource()), telemetry_options(trace_loading: true, collect_metrics: true))
            ->build()
            ->telemetryAttributes();

        static::assertSame(
            ['trace_loading' => true, 'trace_transformations' => false, 'collect_metrics' => true],
            $attributes['telemetry'],
        );
        static::assertSame(MemorySortConfig::class, $attributes['spill']['sort']);
    }
}
