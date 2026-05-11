<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

use Flow\Telemetry\Telemetry;

use function Flow\Telemetry\DSL\resource;
use function Flow\Telemetry\DSL\telemetry;

final class TelemetryStubFactory
{
    public static function create(): Telemetry
    {
        return telemetry(resource());
    }
}
