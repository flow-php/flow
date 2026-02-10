<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Attribute\OsAttribute;
use Flow\Telemetry\Resource\Detector\OsDetector;
use PHPUnit\Framework\TestCase;

final class OsDetectorTest extends TestCase
{
    public function test_detect_returns_expected_os_type_for_current_platform() : void
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
            self::assertSame($expectedType, $osType);
        } else {
            self::assertNotNull($osType);
        }
    }

    public function test_detect_returns_non_empty_os_description() : void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        $osDescription = $resource->get(OsAttribute::DESCRIPTION->value);

        self::assertIsString($osDescription);
        self::assertNotEmpty($osDescription);
    }

    public function test_detect_returns_non_empty_os_name() : void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        $osName = $resource->get(OsAttribute::NAME->value);

        self::assertIsString($osName);
        self::assertNotEmpty($osName);
    }

    public function test_detect_returns_non_empty_os_version() : void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        $osVersion = $resource->get(OsAttribute::VERSION->value);

        self::assertIsString($osVersion);
        self::assertNotEmpty($osVersion);
    }

    public function test_detect_returns_os_attributes() : void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        self::assertTrue($resource->has(OsAttribute::TYPE->value));
        self::assertTrue($resource->has(OsAttribute::NAME->value));
        self::assertTrue($resource->has(OsAttribute::VERSION->value));
        self::assertTrue($resource->has(OsAttribute::DESCRIPTION->value));
    }

    public function test_detected_resource_is_not_empty() : void
    {
        $detector = new OsDetector();
        $resource = $detector->detect();

        self::assertFalse($resource->isEmpty());
        self::assertGreaterThanOrEqual(4, $resource->count());
    }
}
