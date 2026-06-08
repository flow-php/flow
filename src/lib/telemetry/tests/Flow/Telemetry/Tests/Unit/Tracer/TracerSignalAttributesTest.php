<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Tracer;
use PHPUnit\Framework\TestCase;

final class TracerSignalAttributesTest extends TestCase
{
    public function test_default_signal_attributes_are_merged_into_started_spans(): void
    {
        $tracer = new Tracer(
            ResourceMother::default(),
            new InstrumentationScope('test-tracer', '1.0.0'),
            new VoidSpanProcessor(),
            ClockMother::frozen(),
            new MemoryContextStorage(),
            signalAttributes: Attributes::create(['env' => 'prod', 'region' => 'eu']),
        );

        $span = $tracer->span('operation', attributes: ['request.id' => 'abc']);

        static::assertSame('prod', $span->attributes()['env']);
        static::assertSame('eu', $span->attributes()['region']);
        static::assertSame('abc', $span->attributes()['request.id']);
    }

    public function test_per_call_attributes_override_default_signal_attributes(): void
    {
        $tracer = new Tracer(
            ResourceMother::default(),
            new InstrumentationScope('test-tracer', '1.0.0'),
            new VoidSpanProcessor(),
            ClockMother::frozen(),
            new MemoryContextStorage(),
            signalAttributes: Attributes::create(['env' => 'prod']),
        );

        $span = $tracer->span('operation', attributes: ['env' => 'dev']);

        static::assertSame('dev', $span->attributes()['env']);
    }

    public function test_spans_are_unchanged_when_no_default_signal_attributes_configured(): void
    {
        $tracer = new Tracer(
            ResourceMother::default(),
            new InstrumentationScope('test-tracer', '1.0.0'),
            new VoidSpanProcessor(),
            ClockMother::frozen(),
            new MemoryContextStorage(),
        );

        $span = $tracer->span('operation', attributes: ['env' => 'dev']);

        static::assertSame(['env' => 'dev'], $span->attributes());
    }
}
