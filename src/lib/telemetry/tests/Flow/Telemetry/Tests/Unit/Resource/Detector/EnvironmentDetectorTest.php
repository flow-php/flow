<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Attribute\ServiceAttribute;
use Flow\Telemetry\Resource\Detector\EnvironmentDetector;
use PHPUnit\Framework\TestCase;

final class EnvironmentDetectorTest extends TestCase
{
    protected function tearDown() : void
    {
        \putenv('OTEL_SERVICE_NAME');
        \putenv('OTEL_RESOURCE_ATTRIBUTES');
    }

    public function test_detect_allows_empty_values() : void
    {
        \putenv('OTEL_RESOURCE_ATTRIBUTES=key=');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertTrue($resource->has('key'));
        self::assertSame('', $resource->get('key'));
    }

    public function test_detect_handles_escaped_commas_in_values() : void
    {
        \putenv('OTEL_RESOURCE_ATTRIBUTES=key=value\,with\,commas,other=normal');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertTrue($resource->has('key'));
        self::assertTrue($resource->has('other'));
        self::assertSame('value,with,commas', $resource->get('key'));
        self::assertSame('normal', $resource->get('other'));
    }

    public function test_detect_ignores_empty_keys() : void
    {
        \putenv('OTEL_RESOURCE_ATTRIBUTES==value,key=data');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertSame(1, $resource->count());
        self::assertTrue($resource->has('key'));
        self::assertSame('data', $resource->get('key'));
    }

    public function test_detect_ignores_empty_pairs() : void
    {
        \putenv('OTEL_RESOURCE_ATTRIBUTES=key1=value1,,key2=value2');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertSame(2, $resource->count());
        self::assertTrue($resource->has('key1'));
        self::assertTrue($resource->has('key2'));
    }

    public function test_detect_ignores_pairs_without_equals_sign() : void
    {
        \putenv('OTEL_RESOURCE_ATTRIBUTES=key1=value1,invalid,key2=value2');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertSame(2, $resource->count());
        self::assertTrue($resource->has('key1'));
        self::assertTrue($resource->has('key2'));
        self::assertFalse($resource->has('invalid'));
    }

    public function test_detect_parses_otel_resource_attributes() : void
    {
        \putenv('OTEL_RESOURCE_ATTRIBUTES=service.version=1.0.0,deployment.environment.name=production');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertTrue($resource->has('service.version'));
        self::assertTrue($resource->has('deployment.environment.name'));
        self::assertSame('1.0.0', $resource->get('service.version'));
        self::assertSame('production', $resource->get('deployment.environment.name'));
    }

    public function test_detect_reads_otel_service_name() : void
    {
        \putenv('OTEL_SERVICE_NAME=test-service');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertTrue($resource->has(ServiceAttribute::NAME->value));
        self::assertSame('test-service', $resource->get(ServiceAttribute::NAME->value));
    }

    public function test_detect_returns_empty_resource_when_no_env_vars_set() : void
    {
        \putenv('OTEL_SERVICE_NAME');
        \putenv('OTEL_RESOURCE_ATTRIBUTES');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertTrue($resource->isEmpty());
    }

    public function test_detect_trims_whitespace_from_keys_and_values() : void
    {
        \putenv('OTEL_RESOURCE_ATTRIBUTES= key = value , other = data ');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertTrue($resource->has('key'));
        self::assertTrue($resource->has('other'));
        self::assertSame('value', $resource->get('key'));
        self::assertSame('data', $resource->get('other'));
    }

    public function test_otel_service_name_takes_precedence_over_resource_attributes() : void
    {
        \putenv('OTEL_SERVICE_NAME=override-service');
        \putenv('OTEL_RESOURCE_ATTRIBUTES=service.name=original-service');

        $detector = new EnvironmentDetector();
        $resource = $detector->detect();

        self::assertSame('override-service', $resource->get(ServiceAttribute::NAME->value));
    }
}
