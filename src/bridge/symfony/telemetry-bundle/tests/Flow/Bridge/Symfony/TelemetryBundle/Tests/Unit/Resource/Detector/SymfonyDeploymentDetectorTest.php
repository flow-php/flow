<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Resource\Detector;

use Flow\Bridge\Symfony\TelemetryBundle\Resource\Detector\SymfonyDeploymentDetector;
use Flow\Telemetry\Resource\Attribute\DeploymentAttribute;
use PHPUnit\Framework\Attributes\{CoversClass, DataProvider};
use PHPUnit\Framework\TestCase;

#[CoversClass(SymfonyDeploymentDetector::class)]
final class SymfonyDeploymentDetectorTest extends TestCase
{
    /**
     * @return \Generator<string, array{string}>
     */
    public static function kernelEnvironmentProvider() : \Generator
    {
        yield 'dev environment' => ['dev'];
        yield 'prod environment' => ['prod'];
        yield 'test environment' => ['test'];
        yield 'staging environment' => ['staging'];
        yield 'custom environment' => ['custom-env'];
    }

    #[DataProvider('kernelEnvironmentProvider')]
    public function test_detects_deployment_environment_from_kernel_environment(string $kernelEnvironment) : void
    {
        $detector = new SymfonyDeploymentDetector($kernelEnvironment);

        $resource = $detector->detect();

        self::assertTrue($resource->has(DeploymentAttribute::ENVIRONMENT_NAME->value));
        self::assertSame($kernelEnvironment, $resource->get(DeploymentAttribute::ENVIRONMENT_NAME->value));
    }

    public function test_returns_resource_with_single_attribute() : void
    {
        $detector = new SymfonyDeploymentDetector('prod');

        $resource = $detector->detect();

        self::assertSame(1, $resource->count());
        self::assertSame('prod', $resource->get('deployment.environment.name'));
    }
}
