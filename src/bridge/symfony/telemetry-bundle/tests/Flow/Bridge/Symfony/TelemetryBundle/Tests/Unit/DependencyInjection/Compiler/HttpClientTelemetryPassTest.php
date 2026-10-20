<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\HttpClientTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\SuccessHttpClient;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

#[CoversClass(HttpClientTelemetryPass::class)]
final class HttpClientTelemetryPassTest extends TestCase
{
    public function test_decorator_receives_url_sanitizer(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.http_client.enabled', true);
        $container->register('app.http_client', SuccessHttpClient::class)->addTag('http_client.client');

        (new HttpClientTelemetryPass())->process($container);

        static::assertEquals(
            new Reference('flow.telemetry.url_sanitizer'),
            $container->getDefinition('app.http_client.flow_telemetry')->getArgument(3),
        );
    }
}
