<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Detector\StaticDetector;
use PHPUnit\Framework\TestCase;

final class StaticDetectorTest extends TestCase
{
    public function test_detect_returns_empty_resource_for_empty_attributes(): void
    {
        $detector = new StaticDetector([]);
        $resource = $detector->detect();

        static::assertTrue($resource->isEmpty());
    }

    public function test_detect_returns_provided_attributes(): void
    {
        $detector = new StaticDetector([
            'service.name' => 'my-service',
            'service.version' => '1.0.0',
        ]);
        $resource = $detector->detect();

        static::assertSame('my-service', $resource->get('service.name'));
        static::assertSame('1.0.0', $resource->get('service.version'));
    }

    public function test_detect_supports_array_values(): void
    {
        $detector = new StaticDetector([
            'tags' => ['production', 'web'],
        ]);
        $resource = $detector->detect();

        static::assertSame(['production', 'web'], $resource->get('tags'));
    }

    public function test_detect_supports_boolean_values(): void
    {
        $detector = new StaticDetector([
            'enabled' => true,
            'debug' => false,
        ]);
        $resource = $detector->detect();

        static::assertTrue($resource->get('enabled'));
        static::assertFalse($resource->get('debug'));
    }

    public function test_detect_supports_numeric_values(): void
    {
        $detector = new StaticDetector([
            'port' => 8080,
            'ratio' => 0.5,
        ]);
        $resource = $detector->detect();

        static::assertSame(8080, $resource->get('port'));
        static::assertSame(0.5, $resource->get('ratio'));
    }
}
