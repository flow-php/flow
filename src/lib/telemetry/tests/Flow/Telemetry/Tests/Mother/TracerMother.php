<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\SuppressingSampler;
use Flow\Telemetry\Tracer\SpanProcessor;
use Flow\Telemetry\Tracer\Tracer;
use Psr\Clock\ClockInterface;

final class TracerMother
{
    public static function create(
        string $name = 'test-tracer',
        string $version = '1.0.0',
        ?SpanProcessor $processor = null,
        ?ClockInterface $clock = null,
        ?ContextStorage $contextStorage = null,
        ?Resource $resource = null,
    ): Tracer {
        return new Tracer(
            $resource ?? ResourceMother::default(),
            new InstrumentationScope($name, $version),
            $processor ?? new VoidSpanProcessor(),
            $clock ?? ClockMother::frozen(),
            $contextStorage ?? new MemoryContextStorage(),
            new SuppressingSampler(new AlwaysOnSampler()),
        );
    }

    public static function createMemoryProcessor(): MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidExporter());
    }

    public static function withContext(Context $context): Tracer
    {
        return self::create(contextStorage: new MemoryContextStorage($context));
    }

    public static function withContextStorage(ContextStorage $storage): Tracer
    {
        return self::create(contextStorage: $storage);
    }

    public static function withInMemoryProcessor(MemorySpanProcessor $processor): Tracer
    {
        return self::create(processor: $processor);
    }

    public static function withProcessor(SpanProcessor $processor): Tracer
    {
        return self::create(processor: $processor);
    }
}
