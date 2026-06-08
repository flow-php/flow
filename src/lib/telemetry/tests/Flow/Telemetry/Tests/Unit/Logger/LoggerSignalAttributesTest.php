<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class LoggerSignalAttributesTest extends TestCase
{
    public function test_default_signal_attributes_are_merged_into_emitted_records(): void
    {
        $processor = new MemoryLogProcessor(new VoidExporter());
        $logger = new Logger(
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            $processor,
            ClockMother::frozen(),
            new MemoryContextStorage(),
            signalAttributes: Attributes::create(['env' => 'prod', 'region' => 'eu']),
        );

        $logger->info('hello', ['request.id' => 'abc']);

        $attributes = $processor->entries()[0]->record->attributes;
        static::assertSame('prod', $attributes->get('env'));
        static::assertSame('eu', $attributes->get('region'));
        static::assertSame('abc', $attributes->get('request.id'));
    }

    public function test_per_call_attributes_override_default_signal_attributes(): void
    {
        $processor = new MemoryLogProcessor(new VoidExporter());
        $logger = new Logger(
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            $processor,
            ClockMother::frozen(),
            new MemoryContextStorage(),
            signalAttributes: Attributes::create(['env' => 'prod']),
        );

        $logger->info('hello', ['env' => 'dev']);

        static::assertSame('dev', $processor->entries()[0]->record->attributes->get('env'));
    }

    public function test_records_are_unchanged_when_no_default_signal_attributes_configured(): void
    {
        $processor = new MemoryLogProcessor(new VoidExporter());
        $logger = new Logger(
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            $processor,
            ClockMother::frozen(),
            new MemoryContextStorage(),
        );

        $logger->info('hello', ['env' => 'dev']);

        static::assertSame(['env' => 'dev'], $processor->entries()[0]->record->attributes->normalize());
    }
}
