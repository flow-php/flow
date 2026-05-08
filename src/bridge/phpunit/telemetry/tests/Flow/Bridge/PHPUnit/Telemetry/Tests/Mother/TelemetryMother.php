<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Mother;

use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, tracer_provider, void_exporter};

use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tests\Mother\ResourceMother;

final class TelemetryMother
{
    public static function create(?MemorySpanProcessor $spanProcessor = null) : Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        return new Telemetry(
            ResourceMother::default(),
            tracer_provider(
                $spanProcessor ?? memory_span_processor(void_exporter()),
                $clock,
                $contextStorage,
            ),
            meter_provider(
                memory_metric_processor(void_exporter()),
                $clock,
            ),
            logger_provider(
                memory_log_processor(void_exporter()),
                $clock,
                $contextStorage,
            ),
        );
    }

    public static function withSpanProcessor(MemorySpanProcessor $spanProcessor) : Telemetry
    {
        return self::create($spanProcessor);
    }
}
