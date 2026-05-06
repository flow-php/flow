<?php

declare(strict_types=1);

namespace Flow\Telemetry\Signal;

enum SignalType
{
    case LOGS;
    case METRICS;
    case TRACES;
}
