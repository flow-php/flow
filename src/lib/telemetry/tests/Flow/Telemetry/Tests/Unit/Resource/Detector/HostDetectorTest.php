<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Attribute\HostAttribute;
use Flow\Telemetry\Resource\Detector\HostDetector;
use PHPUnit\Framework\TestCase;

final class HostDetectorTest extends TestCase
{
    public function test_detect_returns_host_architecture(): void
    {
        $detector = new HostDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(HostAttribute::ARCH->value));

        $arch = $resource->get(HostAttribute::ARCH->value);

        static::assertIsString($arch);
        static::assertContains($arch, ['amd64', 'arm64', 'arm32', 'x86', 'ia64', 'ppc32', 'ppc64', 's390x']);
    }

    public function test_detect_returns_host_name(): void
    {
        $detector = new HostDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(HostAttribute::NAME->value));

        $hostname = $resource->get(HostAttribute::NAME->value);

        static::assertIsString($hostname);
        static::assertNotEmpty($hostname);
    }

    public function test_detect_returns_resource_with_at_least_two_attributes(): void
    {
        $detector = new HostDetector();
        $resource = $detector->detect();

        static::assertFalse($resource->isEmpty());
        static::assertGreaterThanOrEqual(2, $resource->count());
    }

    public function test_hostname_matches_php_uname(): void
    {
        $detector = new HostDetector();
        $resource = $detector->detect();

        $expectedHostname = \php_uname('n');
        $actualHostname = $resource->get(HostAttribute::NAME->value);

        static::assertSame($expectedHostname, $actualHostname);
    }
}
