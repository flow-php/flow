<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Telemetry;

use Flow\ETL\Foundation\Telemetry\TelemetryAttributes;

enum StepKind
{
    case loading;
    case operator;
    case transformation;

    public function classAttribute(): string
    {
        return match ($this) {
            self::loading => TelemetryAttributes::ATTR_LOADER_CLASS,
            self::operator => TelemetryAttributes::ATTR_OPERATOR_CLASS,
            self::transformation => TelemetryAttributes::ATTR_TRANSFORMER_CLASS,
        };
    }

    public function failure(): string
    {
        return match ($this) {
            self::loading => 'Loading failed',
            self::operator => 'Operator failed',
            self::transformation => 'Transformation failed',
        };
    }

    public function logKey(): string
    {
        return match ($this) {
            self::loading => 'loader',
            self::operator => 'operator',
            self::transformation => 'transformer',
        };
    }
}
