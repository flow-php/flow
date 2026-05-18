<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Severity;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class LogRecordTest extends TestCase
{
    public function test_default_values(): void
    {
        $record = new LogRecord();

        static::assertSame(Severity::INFO, $record->severity);
        static::assertSame('', $record->body);
        static::assertSame([], $record->attributes->normalize());
        static::assertNull($record->timestamp);
    }

    public function test_from_array_with_minimal_data(): void
    {
        $data = [
            'severity' => 9,
            'body' => 'test message',
            'attributes' => [],
            'timestamp' => null,
            'observedTimestamp' => null,
        ];

        $record = LogRecord::fromArray($data);

        static::assertSame(Severity::INFO, $record->severity);
        static::assertSame('test message', $record->body);
        static::assertSame([], $record->attributes->normalize());
        static::assertNull($record->timestamp);
        static::assertNull($record->observedTimestamp);
    }

    public function test_normalize_converts_datetime_immutable_in_attributes(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-01 12:00:00');
        $eventTime = new DateTimeImmutable('2024-01-01 11:59:00');

        $record = new LogRecord(
            body: 'Event occurred',
            attributes: Attributes::create(['event.time' => $eventTime, 'string.attr' => 'value']),
            timestamp: $timestamp,
        );

        $normalized = $record->normalize();

        static::assertSame('2024-01-01T11:59:00+00:00', $normalized['attributes']['event.time']);
        static::assertSame('value', $normalized['attributes']['string.attr']);
    }

    public function test_normalize_from_array_round_trip(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-01 12:00:00');
        $observedTimestamp = new DateTimeImmutable('2024-01-01 12:00:01');

        $original = new LogRecord(
            severity: Severity::ERROR,
            body: 'Database connection failed',
            attributes: Attributes::create(['db.host' => 'localhost', 'db.port' => 5432]),
            timestamp: $timestamp,
            observedTimestamp: $observedTimestamp,
        );

        $normalized = $original->normalize();
        $restored = LogRecord::fromArray($normalized);

        static::assertSame($original->severity, $restored->severity);
        static::assertSame($original->body, $restored->body);
        static::assertSame($original->attributes->normalize(), $restored->attributes->normalize());
        static::assertEquals($original->timestamp, $restored->timestamp);
        static::assertEquals($original->observedTimestamp, $restored->observedTimestamp);
    }

    public function test_normalize_returns_array_representation(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-01 12:00:00');
        $observedTimestamp = new DateTimeImmutable('2024-01-01 12:00:01');

        $record = new LogRecord(
            severity: Severity::WARN,
            body: 'Warning occurred',
            attributes: Attributes::create(['code' => 42, 'message' => 'test']),
            timestamp: $timestamp,
            observedTimestamp: $observedTimestamp,
        );

        $normalized = $record->normalize();

        static::assertEquals(
            [
                'severity' => 13,
                'body' => 'Warning occurred',
                'attributes' => ['code' => 42, 'message' => 'test'],
                'timestamp' => '2024-01-01T12:00:00+00:00',
                'observedTimestamp' => '2024-01-01T12:00:01+00:00',
            ],
            $normalized,
        );
    }

    public function test_observed_timestamp_and_timestamp_are_independent(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-01 10:00:00');
        $observedTimestamp = new DateTimeImmutable('2024-01-01 10:00:01');

        $record = (new LogRecord())
            ->setTimestamp($timestamp)
            ->setObservedTimestamp($observedTimestamp);

        static::assertSame($timestamp, $record->timestamp);
        static::assertSame($observedTimestamp, $record->observedTimestamp);
    }

    public function test_observed_timestamp_default_is_null(): void
    {
        $record = new LogRecord();

        static::assertNull($record->observedTimestamp);
    }

    public function test_set_attribute_accepts_datetime_immutable(): void
    {
        $datetime = new DateTimeImmutable('2024-06-15 12:30:00');
        $record = (new LogRecord())->setAttribute('event.time', $datetime);

        static::assertSame($datetime, $record->attributes->get('event.time'));
    }

    public function test_set_attribute_returns_new_instance(): void
    {
        $record = new LogRecord();

        $newRecord = $record->setAttribute('key', 'value');

        static::assertNotSame($record, $newRecord);
        static::assertSame([], $record->attributes->normalize());
        static::assertSame(['key' => 'value'], $newRecord->attributes->normalize());
    }

    public function test_set_attributes_merges_with_existing(): void
    {
        $record = (new LogRecord())->setAttribute('existing', 'value');

        $newRecord = $record->setAttributes([
            'new1' => 'value1',
            'new2' => 'value2',
        ]);

        static::assertSame(
            [
                'existing' => 'value',
                'new1' => 'value1',
                'new2' => 'value2',
            ],
            $newRecord->attributes->normalize(),
        );
    }

    public function test_set_body_returns_new_instance(): void
    {
        $record = new LogRecord();

        $newRecord = $record->setBody('test message');

        static::assertNotSame($record, $newRecord);
        static::assertSame('', $record->body);
        static::assertSame('test message', $newRecord->body);
    }

    public function test_set_exception_adds_exception_attributes(): void
    {
        $exception = new RuntimeException('Something went wrong');
        $record = (new LogRecord())->setException($exception);

        static::assertSame(RuntimeException::class, $record->attributes->get('exception.type'));
        static::assertSame('Something went wrong', $record->attributes->get('exception.message'));
        $stacktrace = $record->attributes->get('exception.stacktrace');
        static::assertIsString($stacktrace);
        static::assertStringContainsString('LogRecordTest', $stacktrace);
    }

    public function test_set_observed_timestamp_returns_new_instance(): void
    {
        $record = new LogRecord();
        $observedTimestamp = new DateTimeImmutable('2024-06-15 14:30:00');

        $newRecord = $record->setObservedTimestamp($observedTimestamp);

        static::assertNotSame($record, $newRecord);
        static::assertNull($record->observedTimestamp);
        static::assertSame($observedTimestamp, $newRecord->observedTimestamp);
    }

    public function test_set_severity_returns_new_instance(): void
    {
        $record = new LogRecord();

        $newRecord = $record->setSeverity(Severity::ERROR);

        static::assertNotSame($record, $newRecord);
        static::assertSame(Severity::INFO, $record->severity);
        static::assertSame(Severity::ERROR, $newRecord->severity);
    }

    public function test_set_timestamp_returns_new_instance(): void
    {
        $record = new LogRecord();
        $timestamp = new DateTimeImmutable('2024-06-15 12:00:00');

        $newRecord = $record->setTimestamp($timestamp);

        static::assertNotSame($record, $newRecord);
        static::assertNull($record->timestamp);
        static::assertSame($timestamp, $newRecord->timestamp);
    }

    public function test_with_constructor_observed_timestamp(): void
    {
        $observedTimestamp = new DateTimeImmutable('2024-06-15 14:00:00');

        $record = new LogRecord(observedTimestamp: $observedTimestamp);

        static::assertSame($observedTimestamp, $record->observedTimestamp);
    }

    public function test_with_constructor_values(): void
    {
        $timestamp = new DateTimeImmutable('2024-06-15 12:00:00');

        $record = new LogRecord(
            severity: Severity::WARN,
            body: 'warning message',
            attributes: Attributes::create(['code' => 42]),
            timestamp: $timestamp,
        );

        static::assertSame(Severity::WARN, $record->severity);
        static::assertSame('warning message', $record->body);
        static::assertSame(['code' => 42], $record->attributes->normalize());
        static::assertSame($timestamp, $record->timestamp);
    }
}
