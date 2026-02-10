<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Detector\ManualDetector;
use PHPUnit\Framework\TestCase;

final class ManualDetectorTest extends TestCase
{
    public function test_detect_returns_empty_resource_for_empty_attributes() : void
    {
        $detector = new ManualDetector([]);
        $resource = $detector->detect();

        self::assertTrue($resource->isEmpty());
    }

    public function test_detect_returns_provided_attributes() : void
    {
        $detector = new ManualDetector([
            'service.name' => 'my-service',
            'service.version' => '1.0.0',
        ]);
        $resource = $detector->detect();

        self::assertSame('my-service', $resource->get('service.name'));
        self::assertSame('1.0.0', $resource->get('service.version'));
    }

    public function test_detect_supports_array_values() : void
    {
        $detector = new ManualDetector([
            'tags' => ['production', 'web'],
        ]);
        $resource = $detector->detect();

        self::assertSame(['production', 'web'], $resource->get('tags'));
    }

    public function test_detect_supports_boolean_values() : void
    {
        $detector = new ManualDetector([
            'enabled' => true,
            'debug' => false,
        ]);
        $resource = $detector->detect();

        self::assertTrue($resource->get('enabled'));
        self::assertFalse($resource->get('debug'));
    }

    public function test_detect_supports_numeric_values() : void
    {
        $detector = new ManualDetector([
            'port' => 8080,
            'ratio' => 0.5,
        ]);
        $resource = $detector->detect();

        self::assertSame(8080, $resource->get('port'));
        self::assertSame(0.5, $resource->get('ratio'));
    }
}
