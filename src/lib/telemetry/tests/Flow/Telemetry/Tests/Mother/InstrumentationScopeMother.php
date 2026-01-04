<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\InstrumentationScope;

final class InstrumentationScopeMother
{
    public static function default() : InstrumentationScope
    {
        return new InstrumentationScope('test-instrumentation', '1.0.0');
    }

    public static function named(string $name, string $version = '1.0.0') : InstrumentationScope
    {
        return new InstrumentationScope($name, $version);
    }
}
