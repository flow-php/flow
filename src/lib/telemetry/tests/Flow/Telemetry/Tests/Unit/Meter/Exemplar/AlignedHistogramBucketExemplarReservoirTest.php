<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Exemplar;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Meter\Exemplar\AlignedHistogramBucketExemplarReservoir;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class AlignedHistogramBucketExemplarReservoirTest extends TestCase
{
    public function test_can_fill_all_buckets(): void
    {
        $bucketCount = 5;
        $reservoir = new AlignedHistogramBucketExemplarReservoir($bucketCount);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        for ($i = 0; $i < $bucketCount; $i++) {
            $reservoir->offer($i * 10, [], $context, $timestamp, bucketIndex: $i);
        }

        $exemplars = $reservoir->collect();
        static::assertCount($bucketCount, $exemplars);

        $values = \array_map(static fn($e) => $e->value, $exemplars);
        \sort($values);
        static::assertSame([0, 10, 20, 30, 40], $values);
    }

    public function test_collect_returns_only_non_null_bucket_exemplars(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(5);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(10, [], $context, $timestamp, bucketIndex: 1);
        $reservoir->offer(20, [], $context, $timestamp, bucketIndex: 3);

        $exemplars = $reservoir->collect();
        static::assertCount(2, $exemplars);
    }

    public function test_collect_with_reset_clears_all_buckets(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(3);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(10, [], $context, $timestamp, bucketIndex: 0);
        $reservoir->offer(20, [], $context, $timestamp, bucketIndex: 1);
        $reservoir->offer(30, [], $context, $timestamp, bucketIndex: 2);

        $firstCollect = $reservoir->collect(reset: true);
        static::assertCount(3, $firstCollect);

        $secondCollect = $reservoir->collect();
        static::assertCount(0, $secondCollect);
    }

    public function test_collect_without_reset_keeps_bucket_exemplars(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(2);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, [], $context, $timestamp, bucketIndex: 0);

        $firstCollect = $reservoir->collect(reset: false);
        static::assertCount(1, $firstCollect);

        $secondCollect = $reservoir->collect(reset: false);
        static::assertCount(1, $secondCollect);
    }

    public function test_exemplar_stores_correct_trace_context(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(3);
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $context = SpanContext::create($traceId, $spanId, null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');

        $reservoir->offer(42.5, ['key' => 'value'], $context, $timestamp, bucketIndex: 1);

        $exemplars = $reservoir->collect();
        static::assertCount(1, $exemplars);
        static::assertSame($traceId->toHex(), $exemplars[0]->traceId->toHex());
        static::assertSame($spanId->toHex(), $exemplars[0]->spanId->toHex());
        static::assertSame(42.5, $exemplars[0]->value);
        static::assertEquals($timestamp, $exemplars[0]->timestamp);
        static::assertSame(['key' => 'value'], $exemplars[0]->filteredAttributes);
    }

    public function test_offer_ignores_bucket_index_beyond_count(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(3);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, [], $context, $timestamp, bucketIndex: 3);
        $reservoir->offer(200, [], $context, $timestamp, bucketIndex: 10);

        $exemplars = $reservoir->collect();
        static::assertCount(0, $exemplars);
    }

    public function test_offer_ignores_negative_bucket_index(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(3);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, [], $context, $timestamp, bucketIndex: -1);

        $exemplars = $reservoir->collect();
        static::assertCount(0, $exemplars);
    }

    public function test_offer_replaces_existing_exemplar_in_same_bucket(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(3);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, ['first' => true], $context, $timestamp, bucketIndex: 1);
        $reservoir->offer(200, ['second' => true], $context, $timestamp, bucketIndex: 1);

        $exemplars = $reservoir->collect();
        static::assertCount(1, $exemplars);
        static::assertSame(200, $exemplars[0]->value);
        static::assertSame(['second' => true], $exemplars[0]->filteredAttributes);
    }

    public function test_offer_stores_exemplar_in_correct_bucket(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(5);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(100, ['http.method' => 'GET'], $context, $timestamp, bucketIndex: 2);

        $exemplars = $reservoir->collect();
        static::assertCount(1, $exemplars);
        static::assertSame(100, $exemplars[0]->value);
        static::assertSame(['http.method' => 'GET'], $exemplars[0]->filteredAttributes);
    }

    public function test_offer_stores_exemplars_in_different_buckets(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(5);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(10, [], $context, $timestamp, bucketIndex: 0);
        $reservoir->offer(20, [], $context, $timestamp, bucketIndex: 2);
        $reservoir->offer(30, [], $context, $timestamp, bucketIndex: 4);

        $exemplars = $reservoir->collect();
        static::assertCount(3, $exemplars);

        $values = \array_map(static fn($e) => $e->value, $exemplars);
        static::assertContains(10, $values);
        static::assertContains(20, $values);
        static::assertContains(30, $values);
    }

    public function test_reset_clears_all_buckets(): void
    {
        $reservoir = new AlignedHistogramBucketExemplarReservoir(3);
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());
        $timestamp = new \DateTimeImmutable();

        $reservoir->offer(10, [], $context, $timestamp, bucketIndex: 0);
        $reservoir->offer(20, [], $context, $timestamp, bucketIndex: 1);

        $reservoir->reset();

        $exemplars = $reservoir->collect();
        static::assertCount(0, $exemplars);
    }
}
