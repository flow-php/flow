<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Context\{Context, ContextStorage, MemoryContextStorage};
use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\{VoidSpanExporter, VoidSpanProcessor};
use Flow\Telemetry\Tracer\{SpanProcessor, Tracer};
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
    ) : Tracer {
        return new Tracer(
            $resource ?? ResourceMother::default(),
            new InstrumentationScope($name, $version),
            $processor ?? new VoidSpanProcessor(),
            $clock ?? ClockMother::frozen(),
            $contextStorage ?? new MemoryContextStorage(),
        );
    }

    public static function createMemoryProcessor() : MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidSpanExporter());
    }

    public static function withContext(Context $context) : Tracer
    {
        return self::create(contextStorage: new MemoryContextStorage($context));
    }

    public static function withContextStorage(ContextStorage $storage) : Tracer
    {
        return self::create(contextStorage: $storage);
    }

    public static function withInMemoryProcessor(MemorySpanProcessor $processor) : Tracer
    {
        return self::create(processor: $processor);
    }

    public static function withProcessor(SpanProcessor $processor) : Tracer
    {
        return self::create(processor: $processor);
    }
}
