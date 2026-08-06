<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Psr18\Telemetry\PSR18TraceableClient;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\Psr18ClientTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Psr18\MockPsr18Client;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Psr18\ResettablePsr18Client;
use Flow\Telemetry\Telemetry;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use stdClass;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Contracts\Service\ResetInterface;

#[CoversClass(Psr18ClientTelemetryPass::class)]
final class Psr18ClientTelemetryPassTest extends TestCase
{
    public function test_autoconfigured_client_compiles_and_is_traced(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->registerForAutoconfiguration(ResetInterface::class)->addTag('kernel.reset', ['method' => 'reset']);
        $container->register('app.http_client', ResettablePsr18Client::class)->setAutoconfigured(true)->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertSame(PSR18TraceableClient::class, $container->getDefinition('app.http_client')->getClass());
    }

    public function test_child_of_an_abstract_psr18_parent_compiles_and_is_traced(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.base_client', MockPsr18Client::class)->setAbstract(true);
        $container->setDefinition('app.child_client', (new ChildDefinition('app.base_client'))->setPublic(true));
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertSame(PSR18TraceableClient::class, $container->getDefinition('app.child_client')->getClass());
    }

    public function test_plain_client_is_traced(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('psr18.http_client', MockPsr18Client::class)->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertSame(PSR18TraceableClient::class, $container->getDefinition('psr18.http_client')->getClass());
    }

    public function test_client_declared_as_a_parameter_is_traced(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->setParameter('app.client.class', MockPsr18Client::class);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.client', '%app.client.class%')->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertSame(PSR18TraceableClient::class, $container->getDefinition('app.client')->getClass());
    }

    public function test_non_existent_class_is_left_untouched(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.ghost', 'Flow\Bridge\Symfony\TelemetryBundle\Tests\NoSuchClient')->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertFalse($container->hasDefinition('app.ghost.flow_telemetry'));
        static::assertSame(
            'Flow\Bridge\Symfony\TelemetryBundle\Tests\NoSuchClient',
            $container->getDefinition('app.ghost')->getClass(),
        );
    }

    public function test_non_psr18_class_is_left_untouched(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.thing', stdClass::class)->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertFalse($container->hasDefinition('app.thing.flow_telemetry'));
        static::assertSame(stdClass::class, $container->getDefinition('app.thing')->getClass());
    }

    public function test_traceable_client_is_not_double_wrapped(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.traced', PSR18TraceableClient::class)->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertFalse($container->hasDefinition('app.traced.flow_telemetry'));
    }

    public function test_client_excluded_by_exact_id_is_left_untouched(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->setParameter('flow.telemetry.psr18_client.exclude_clients', ['app.client']);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.client', MockPsr18Client::class)->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertFalse($container->hasDefinition('app.client.flow_telemetry'));
        static::assertSame(MockPsr18Client::class, $container->getDefinition('app.client')->getClass());
    }

    public function test_client_excluded_by_regex_is_left_untouched(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', true);
        $container->setParameter('flow.telemetry.psr18_client.exclude_clients', ['/^app\..*/']);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.client', MockPsr18Client::class)->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertFalse($container->hasDefinition('app.client.flow_telemetry'));
        static::assertSame(MockPsr18Client::class, $container->getDefinition('app.client')->getClass());
    }

    public function test_nothing_is_decorated_when_disabled(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.psr18_client.enabled', false);
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.client', MockPsr18Client::class)->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertFalse($container->hasDefinition('app.client.flow_telemetry'));
        static::assertSame(MockPsr18Client::class, $container->getDefinition('app.client')->getClass());
    }

    public function test_nothing_is_decorated_when_the_parameter_is_absent(): void
    {
        $container = new ContainerBuilder();
        $container->register(Telemetry::class)->setSynthetic(true);
        $container->register('app.client', MockPsr18Client::class)->setPublic(true);
        $container->addCompilerPass(new Psr18ClientTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertFalse($container->hasDefinition('app.client.flow_telemetry'));
        static::assertSame(MockPsr18Client::class, $container->getDefinition('app.client')->getClass());
    }
}
