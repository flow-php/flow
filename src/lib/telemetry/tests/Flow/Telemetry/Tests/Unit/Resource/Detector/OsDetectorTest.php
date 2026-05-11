<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Attribute\OsAttribute;
use Flow\Telemetry\Resource\Detector\OsDetector;
use PHPUnit\Framework\TestCase;

final class OsDetectorTest extends TestCase
{
    public function test_detect_returns_expected_os_type_for_current_platform(): void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        $osType = $resource->get(OsAttribute::TYPE->value);

        $expectedType = match (PHP_OS_FAMILY) {
            'Darwin' => 'darwin',
            'Windows' => 'windows',
            'Linux' => 'linux',
            'BSD' => null,
            'Solaris' => 'solaris',
            default => null,
        };

        if ($expectedType !== null) {
            static::assertSame($expectedType, $osType);
        } else {
            static::assertNotNull($osType);
        }
    }

    public function test_detect_returns_non_empty_os_description(): void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        $osDescription = $resource->get(OsAttribute::DESCRIPTION->value);

        static::assertIsString($osDescription);
        static::assertNotEmpty($osDescription);
    }

    public function test_detect_returns_non_empty_os_name(): void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        $osName = $resource->get(OsAttribute::NAME->value);

        static::assertIsString($osName);
        static::assertNotEmpty($osName);
    }

    public function test_detect_returns_non_empty_os_version(): void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        $osVersion = $resource->get(OsAttribute::VERSION->value);

        static::assertIsString($osVersion);
        static::assertNotEmpty($osVersion);
    }

    public function test_detect_returns_os_attributes(): void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(OsAttribute::TYPE->value));
        static::assertTrue($resource->has(OsAttribute::NAME->value));
        static::assertTrue($resource->has(OsAttribute::VERSION->value));
        static::assertTrue($resource->has(OsAttribute::DESCRIPTION->value));
    }

    public function test_detected_resource_is_not_empty(): void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        static::assertFalse($resource->isEmpty());
        static::assertGreaterThanOrEqual(4, $resource->count());
    }
}
