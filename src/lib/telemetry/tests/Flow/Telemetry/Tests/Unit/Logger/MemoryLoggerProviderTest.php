<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\LogRecordLimits;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class MemoryLoggerProviderTest extends TestCase
{
    private Resource $resource;

    protected function setUp(): void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_creates_new_logger_each_time(): void
    {
        $provider = new LoggerProvider($this->createProcessor(), ClockMother::frozen(), new MemoryContextStorage());

        $logger1 = $provider->logger($this->resource, 'service-a', '1.0');
        $logger2 = $provider->logger($this->resource, 'service-a', '1.0');

        static::assertNotSame($logger1, $logger2);
    }

    public function test_limits_attribute_count_drops_excess_attributes(): void
    {
        $processor = $this->createProcessor();
        $limits = new LogRecordLimits(attributeCountLimit: 2);
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage(), $limits);
        $logger = $provider->logger($this->resource, 'service', '1.0');

        $logger->info('message', [
            'key1' => 'value1',
            'key2' => 'value2',
            'key3' => 'value3',
            'key4' => 'value4',
        ]);

        $entry = $processor->entries()[0];
        static::assertCount(2, $entry->record->attributes->normalize());
        static::assertSame(2, $entry->droppedAttributeCount);
        static::assertArrayHasKey('key1', $entry->record->attributes->normalize());
        static::assertArrayHasKey('key2', $entry->record->attributes->normalize());
    }

    public function test_limits_attribute_value_length_truncates_strings(): void
    {
        $processor = $this->createProcessor();
        $limits = new LogRecordLimits(attributeValueLengthLimit: 10);
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage(), $limits);
        $logger = $provider->logger($this->resource, 'service', '1.0');

        $logger->info('message', [
            'short' => 'abc',
            'long' => 'this-is-a-very-long-value',
        ]);

        $entry = $processor->entries()[0];
        $attrs = $entry->record->attributes->normalize();
        static::assertSame('abc', $attrs['short']);
        static::assertSame('this-is-a-', $attrs['long']);
    }

    public function test_limits_dropped_count_in_normalized_output(): void
    {
        $processor = $this->createProcessor();
        $limits = new LogRecordLimits(attributeCountLimit: 1);
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage(), $limits);
        $logger = $provider->logger($this->resource, 'service', '1.0');

        $logger->info('message', ['key1' => 'v1', 'key2' => 'v2', 'key3' => 'v3']);

        $entry = $processor->entries()[0];
        $normalized = $entry->normalize();
        static::assertSame(2, $normalized['droppedAttributeCount']);
    }

    public function test_limits_with_default_limits_no_truncation(): void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'service', '1.0');

        $attrs = [];

        for ($i = 0; $i < 100; $i++) {
            $attrs["key{$i}"] = 'value';
        }
        $logger->info('message', $attrs);

        $entry = $processor->entries()[0];
        static::assertCount(100, $entry->record->attributes->normalize());
        static::assertSame(0, $entry->droppedAttributeCount);
    }

    public function test_logger_accepts_attributes_object(): void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'service', '1.0');

        $logger->info('message with attributes', Attributes::create(['key' => 'value']));

        static::assertCount(1, $processor->entries());
        static::assertSame('value', $processor->entries()[0]->record->attributes->normalize()['key']);
    }

    public function test_logger_accepts_custom_observed_timestamp(): void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'service', '1.0');
        $observedTimestamp = new DateTimeImmutable('2024-01-15 10:35:00');

        $logger->info('message with observed timestamp', [], null, $observedTimestamp);

        static::assertCount(1, $processor->entries());
        static::assertEquals($observedTimestamp, $processor->entries()[0]->record->observedTimestamp);
    }

    public function test_logger_accepts_custom_span_context(): void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'service', '1.0');
        $customSpanContext = SpanContext::create(TraceId::generate(), SpanId::generate());

        $logger->info('message with custom span context', [], null, null, $customSpanContext);

        static::assertCount(1, $processor->entries());
        static::assertSame($customSpanContext, $processor->entries()[0]->spanContext);
    }

    public function test_logger_accepts_custom_timestamp(): void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'service', '1.0');
        $customTimestamp = new DateTimeImmutable('2024-01-15 10:30:00');

        $logger->info('message with custom timestamp', [], $customTimestamp);

        static::assertCount(1, $processor->entries());
        static::assertEquals($customTimestamp, $processor->entries()[0]->timestamp);
    }

    public function test_logger_passes_all_custom_parameters_for_all_severity_levels(): void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'service', '1.0');
        $timestamp = new DateTimeImmutable('2024-01-15 10:30:00');
        $observedTimestamp = new DateTimeImmutable('2024-01-15 10:35:00');
        $spanContext = SpanContext::create(TraceId::generate(), SpanId::generate());

        $logger->trace('trace msg', [], $timestamp, $observedTimestamp, $spanContext);
        $logger->debug('debug msg', [], $timestamp, $observedTimestamp, $spanContext);
        $logger->info('info msg', [], $timestamp, $observedTimestamp, $spanContext);
        $logger->warn('warn msg', [], $timestamp, $observedTimestamp, $spanContext);
        $logger->error('error msg', [], $timestamp, $observedTimestamp, $spanContext);
        $logger->fatal('fatal msg', [], $timestamp, $observedTimestamp, $spanContext);

        static::assertCount(6, $processor->entries());

        foreach ($processor->entries() as $entry) {
            static::assertEquals($timestamp, $entry->timestamp);
            static::assertEquals($observedTimestamp, $entry->record->observedTimestamp);
            static::assertSame($spanContext, $entry->spanContext);
        }
    }

    public function test_logger_returns_logger_instance(): void
    {
        $provider = new LoggerProvider($this->createProcessor(), ClockMother::frozen(), new MemoryContextStorage());

        $logger = $provider->logger($this->resource, 'test', '1.0');

        static::assertInstanceOf(Logger::class, $logger);
    }

    public function test_processor_flush_returns_true(): void
    {
        $processor = $this->createProcessor();

        static::assertTrue($processor->flush());
    }

    public function test_processor_stores_logs_from_all_loggers(): void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());

        $logger1 = $provider->logger($this->resource, 'service-a', '1.0');
        $logger2 = $provider->logger($this->resource, 'service-b', '2.0');

        $logger1->info('message from service A');
        $logger2->error('message from service B');

        static::assertCount(2, $processor->entries());
        static::assertSame(Severity::INFO, $processor->entries()[0]->record->severity);
        static::assertSame(Severity::ERROR, $processor->entries()[1]->record->severity);
    }

    private function createProcessor(): MemoryLogProcessor
    {
        return new MemoryLogProcessor(new VoidExporter());
    }
}
