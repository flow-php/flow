<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\InterfaceAliasRepointer;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TraceableCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache\ArrayCacheAdapter;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RequiresMethod;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\Alias;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Contracts\Cache\CacheInterface;
use Symfony\Contracts\Cache\NamespacedPoolInterface;

#[CoversClass(InterfaceAliasRepointer::class)]
final class InterfaceAliasRepointerTest extends TestCase
{
    #[RequiresMethod(NamespacedPoolInterface::class, 'withSubNamespace')]
    public function test_alias_the_decorator_cannot_satisfy_is_pointed_at_the_inner_service(): void
    {
        $container = new ContainerBuilder();
        $container->register('cache.app', ArrayCacheAdapter::class);
        $container->setAlias(NamespacedPoolInterface::class, 'cache.app');

        (new InterfaceAliasRepointer($container))->repoint(
            'cache.app',
            'cache.app.flow_telemetry.inner',
            TraceableCacheAdapter::class,
        );

        static::assertSame(
            'cache.app.flow_telemetry.inner',
            (string) $container->getAlias(NamespacedPoolInterface::class),
        );
    }

    public function test_alias_the_decorator_satisfies_is_left_alone(): void
    {
        $container = new ContainerBuilder();
        $container->register('cache.app', ArrayCacheAdapter::class);
        $container->setAlias(CacheInterface::class, 'cache.app');

        (new InterfaceAliasRepointer($container))->repoint(
            'cache.app',
            'cache.app.flow_telemetry.inner',
            TraceableCacheAdapter::class,
        );

        static::assertSame('cache.app', (string) $container->getAlias(CacheInterface::class));
    }

    #[RequiresMethod(NamespacedPoolInterface::class, 'withSubNamespace')]
    public function test_alias_pointing_at_another_service_is_left_alone(): void
    {
        $container = new ContainerBuilder();
        $container->register('cache.other', ArrayCacheAdapter::class);
        $container->setAlias(NamespacedPoolInterface::class, 'cache.other');

        (new InterfaceAliasRepointer($container))->repoint(
            'cache.app',
            'cache.app.flow_telemetry.inner',
            TraceableCacheAdapter::class,
        );

        static::assertSame('cache.other', (string) $container->getAlias(NamespacedPoolInterface::class));
    }

    public function test_alias_that_is_not_an_interface_is_left_alone(): void
    {
        $container = new ContainerBuilder();
        $container->register('cache.app', ArrayCacheAdapter::class);
        $container->setAlias('cache.app.alias', 'cache.app');

        (new InterfaceAliasRepointer($container))->repoint(
            'cache.app',
            'cache.app.flow_telemetry.inner',
            TraceableCacheAdapter::class,
        );

        static::assertSame('cache.app', (string) $container->getAlias('cache.app.alias'));
    }

    #[RequiresMethod(NamespacedPoolInterface::class, 'withSubNamespace')]
    public function test_public_visibility_is_preserved(): void
    {
        $container = new ContainerBuilder();
        $container->register('cache.app', ArrayCacheAdapter::class);
        $container->setAlias(NamespacedPoolInterface::class, new Alias('cache.app', true));

        (new InterfaceAliasRepointer($container))->repoint(
            'cache.app',
            'cache.app.flow_telemetry.inner',
            TraceableCacheAdapter::class,
        );

        static::assertTrue($container->getAlias(NamespacedPoolInterface::class)->isPublic());
    }

    #[RequiresMethod(NamespacedPoolInterface::class, 'withSubNamespace')]
    public function test_private_visibility_is_preserved(): void
    {
        $container = new ContainerBuilder();
        $container->register('cache.app', ArrayCacheAdapter::class);
        $container->setAlias(NamespacedPoolInterface::class, new Alias('cache.app', false));

        (new InterfaceAliasRepointer($container))->repoint(
            'cache.app',
            'cache.app.flow_telemetry.inner',
            TraceableCacheAdapter::class,
        );

        static::assertFalse($container->getAlias(NamespacedPoolInterface::class)->isPublic());
    }

    #[RequiresMethod(NamespacedPoolInterface::class, 'withSubNamespace')]
    public function test_deprecation_is_preserved(): void
    {
        $container = new ContainerBuilder();
        $container->register('cache.app', ArrayCacheAdapter::class);
        $container->setAlias(NamespacedPoolInterface::class, 'cache.app')->setDeprecated(
            'acme/pkg',
            '1.2',
            'The "%alias_id%" alias is deprecated.',
        );

        (new InterfaceAliasRepointer($container))->repoint(
            'cache.app',
            'cache.app.flow_telemetry.inner',
            TraceableCacheAdapter::class,
        );

        $alias = $container->getAlias(NamespacedPoolInterface::class);

        static::assertTrue($alias->isDeprecated());
        static::assertSame('acme/pkg', $alias->getDeprecation('x')['package']);
    }
}
