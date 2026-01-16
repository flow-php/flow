<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Exemplar;

use Flow\Telemetry\Context\{SpanId, TraceFlags, TraceId};
use Flow\Telemetry\Meter\Exemplar\SimpleFixedSizeExemplarReservoir;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class SimpleFixedSizeExemplarReservoirTest extends TestCase
{
    public function test_bucket_index_is_ignored() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(2);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, [], $context, $timestamp, bucketIndex: 5);
        $reservoir->offer(200, [], $context, $timestamp, bucketIndex: 10);

        $exemplars = $reservoir->collect();
        self::assertCount(2, $exemplars);
    }

    public function test_collect_returns_all_stored_exemplars() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(3);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(10, [], $context, $timestamp);
        $reservoir->offer(20, [], $context, $timestamp);
        $reservoir->offer(30, [], $context, $timestamp);

        $exemplars = $reservoir->collect();
        self::assertCount(3, $exemplars);
    }

    public function test_collect_with_reset_clears_reservoir() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(2);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, [], $context, $timestamp);
        $reservoir->offer(200, [], $context, $timestamp);

        $firstCollect = $reservoir->collect(reset: true);
        self::assertCount(2, $firstCollect);

        $secondCollect = $reservoir->collect();
        self::assertCount(0, $secondCollect);
    }

    public function test_collect_without_reset_keeps_exemplars() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(2);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, [], $context, $timestamp);

        $firstCollect = $reservoir->collect(reset: false);
        self::assertCount(1, $firstCollect);

        $secondCollect = $reservoir->collect(reset: false);
        self::assertCount(1, $secondCollect);
    }

    public function test_exemplar_stores_correct_trace_context() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(1);
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $context = SpanContext::create($traceId, $spanId, null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');

        $reservoir->offer(42.5, ['key' => 'value'], $context, $timestamp);

        $exemplars = $reservoir->collect();
        self::assertCount(1, $exemplars);
        self::assertSame($traceId->toHex(), $exemplars[0]->traceId->toHex());
        self::assertSame($spanId->toHex(), $exemplars[0]->spanId->toHex());
        self::assertSame(42.5, $exemplars[0]->value);
        self::assertEquals($timestamp, $exemplars[0]->timestamp);
    }

    public function test_offer_stores_exemplar_when_reservoir_has_space() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(3);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, ['http.method' => 'GET'], $context, $timestamp);

        $exemplars = $reservoir->collect();
        self::assertCount(1, $exemplars);
        self::assertSame(100, $exemplars[0]->value);
        self::assertSame(['http.method' => 'GET'], $exemplars[0]->filteredAttributes);
        self::assertSame($context->traceId->toHex(), $exemplars[0]->traceId->toHex());
        self::assertSame($context->spanId->toHex(), $exemplars[0]->spanId->toHex());
    }

    public function test_reservoir_sampling_replaces_with_probability() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(2);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        for ($i = 0; $i < 1000; $i++) {
            $reservoir->offer($i, [], $context, $timestamp);
        }

        $exemplars = $reservoir->collect();
        self::assertCount(2, $exemplars);

        $values = \array_map(fn ($e) => $e->value, $exemplars);
        self::assertCount(2, \array_unique($values));
    }

    public function test_reservoir_with_size_one() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(1);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, [], $context, $timestamp);
        $reservoir->offer(200, [], $context, $timestamp);
        $reservoir->offer(300, [], $context, $timestamp);

        $exemplars = $reservoir->collect();
        self::assertCount(1, $exemplars);
    }

    public function test_reset_clears_all_exemplars() : void
    {
        $reservoir = new SimpleFixedSizeExemplarReservoir(2);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, [], $context, $timestamp);
        $reservoir->offer(200, [], $context, $timestamp);

        $reservoir->reset();

        $exemplars = $reservoir->collect();
        self::assertCount(0, $exemplars);
    }
}
