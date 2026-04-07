<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

use function Flow\Telemetry\DSL\{resource, telemetry};
use Flow\Telemetry\Telemetry;

final class TelemetryStubFactory
{
    public static function create() : Telemetry
    {
        return telemetry(resource());
    }
}
