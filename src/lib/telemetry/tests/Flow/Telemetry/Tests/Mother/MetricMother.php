<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;

final class MetricMother
{
    public static function counter(
        string $name,
        int|float $value,
        ?string $unit = null,
        ?string $description = null,
    ): Metric {
        return new Metric(
            name: $name,
            type: MetricType::COUNTER,
            value: $value,
            attributes: Attributes::empty(),
            timestamp: new DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
            unit: $unit,
            description: $description,
        );
    }

    public static function deterministicCounter(
        string $name,
        int|float $value,
        ?string $unit = null,
        ?string $description = null,
    ): Metric {
        return new Metric(
            name: $name,
            type: MetricType::COUNTER,
            value: $value,
            attributes: Attributes::empty(),
            timestamp: new DateTimeImmutable('2024-01-15T10:30:00.000000+00:00'),
            resource: ResourceMother::full(),
            scope: InstrumentationScopeMother::default(),
            unit: $unit,
            description: $description,
        );
    }

    public static function deterministicGauge(
        string $name,
        int|float $value,
        ?string $unit = null,
        ?string $description = null,
    ): Metric {
        return new Metric(
            name: $name,
            type: MetricType::GAUGE,
            value: $value,
            attributes: Attributes::empty(),
            timestamp: new DateTimeImmutable('2024-01-15T10:30:00.000000+00:00'),
            resource: ResourceMother::full(),
            scope: InstrumentationScopeMother::default(),
            unit: $unit,
            description: $description,
        );
    }
}
