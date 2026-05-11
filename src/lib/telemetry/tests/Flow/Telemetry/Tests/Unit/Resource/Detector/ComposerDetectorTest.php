<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Composer\InstalledVersions;
use Flow\Telemetry\Resource\Attribute\ServiceAttribute;
use Flow\Telemetry\Resource\Detector\ComposerDetector;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_string;

final class ComposerDetectorTest extends TestCase
{
    public function test_detect_extracts_package_name_from_vendor_prefix(): void
    {
        if (!\class_exists(InstalledVersions::class)) {
            static::markTestSkipped('Composer InstalledVersions not available');
        }

        $detector = new ComposerDetector();
        $resource = $detector->detect();

        $serviceName = $resource->get(ServiceAttribute::NAME->value);

        static::assertNotNull($serviceName);
        static::assertStringNotContainsString('/', type_string()->assert($serviceName));
    }

    public function test_detect_returns_service_name_from_root_package(): void
    {
        if (!\class_exists(InstalledVersions::class)) {
            static::markTestSkipped('Composer InstalledVersions not available');
        }

        $detector = new ComposerDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ServiceAttribute::NAME->value));

        $serviceName = $resource->get(ServiceAttribute::NAME->value);

        static::assertIsString($serviceName);
        static::assertNotEmpty($serviceName);
    }

    public function test_detect_returns_service_version_from_root_package(): void
    {
        if (!\class_exists(InstalledVersions::class)) {
            static::markTestSkipped('Composer InstalledVersions not available');
        }

        $detector = new ComposerDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ServiceAttribute::VERSION->value));

        $version = $resource->get(ServiceAttribute::VERSION->value);

        static::assertIsString($version);
    }
}
