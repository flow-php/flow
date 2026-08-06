<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\CacheTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TagAwareTraceableCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TraceableCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\CachePoolContainerMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Cache\Adapter\RedisTagAwareAdapter;
use Symfony\Component\DependencyInjection\Argument\IteratorArgument;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\Reference;

#[CoversClass(CacheTelemetryPass::class)]
final class CacheTelemetryPassTest extends TestCase
{
    public function test_abstract_adapter_templates_are_skipped(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();

        (new CacheTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('cache.adapter.system.flow_telemetry'));
        static::assertFalse($container->hasDefinition('cache.adapter.filesystem.flow_telemetry'));
    }

    public function test_pool_inheriting_its_class_from_a_parent_is_traced(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();

        (new CacheTelemetryPass())->process($container);

        static::assertSame(
            TraceableCacheAdapter::class,
            $container->getDefinition('cache.system.flow_telemetry')->getClass(),
        );
        static::assertSame(
            TraceableCacheAdapter::class,
            $container->getDefinition('cache.validator.flow_telemetry')->getClass(),
        );
    }

    public function test_pool_inheriting_through_multiple_levels_is_traced(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();

        (new CacheTelemetryPass())->process($container);

        static::assertSame(
            TraceableCacheAdapter::class,
            $container->getDefinition('cache.doctrine.orm.default.result.flow_telemetry')->getClass(),
        );
    }

    public function test_tag_aware_pool_gets_the_tag_aware_decorator(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();

        (new CacheTelemetryPass())->process($container);

        static::assertSame(
            TagAwareTraceableCacheAdapter::class,
            $container->getDefinition('app.redis_pool.flow_telemetry')->getClass(),
        );
    }

    public function test_pool_declared_as_a_parameter_is_traced(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();

        (new CacheTelemetryPass())->process($container);

        static::assertSame(
            TraceableCacheAdapter::class,
            $container->getDefinition('app.param_pool.flow_telemetry')->getClass(),
        );
    }

    public function test_tag_aware_pool_declared_as_a_parameter_gets_the_tag_aware_decorator(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();
        $container->setParameter('app.tag_aware_pool.class', RedisTagAwareAdapter::class);
        $container->register('app.param_tag_aware_pool', '%app.tag_aware_pool.class%')->addTag('cache.pool');

        (new CacheTelemetryPass())->process($container);

        static::assertSame(
            TagAwareTraceableCacheAdapter::class,
            $container->getDefinition('app.param_tag_aware_pool.flow_telemetry')->getClass(),
        );
    }

    public function test_pool_excluded_by_exact_id_is_left_untouched(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();
        $container->setParameter('flow.telemetry.cache.exclude_pools', ['cache.system']);

        (new CacheTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('cache.system.flow_telemetry'));
        static::assertTrue($container->hasDefinition('cache.validator.flow_telemetry'));
    }

    public function test_pool_excluded_by_regex_is_left_untouched(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();
        $container->setParameter('flow.telemetry.cache.exclude_pools', ['/^cache\.validator.*/']);

        (new CacheTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('cache.validator.flow_telemetry'));
        static::assertTrue($container->hasDefinition('cache.system.flow_telemetry'));
    }

    public function test_nothing_is_decorated_when_disabled(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();
        $container->setParameter('flow.telemetry.cache.enabled', false);
        $container->setParameter('flow.telemetry.cache.flush_deferred', true);

        (new CacheTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('cache.system.flow_telemetry'));
        static::assertFalse($container->hasDefinition('app.redis_pool.flow_telemetry'));
        static::assertFalse($container->hasDefinition('flow.telemetry.cache.deferred_flush_subscriber'));
    }

    public function test_deferred_flush_subscriber_collects_the_newly_traced_pools(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();
        $container->setParameter('flow.telemetry.cache.flush_deferred', true);

        (new CacheTelemetryPass())->process($container);

        /** @var mixed $innerPools */
        $innerPools = $container->getDefinition('flow.telemetry.cache.deferred_flush_subscriber')->getArgument(0);

        static::assertInstanceOf(IteratorArgument::class, $innerPools);
        static::assertContainsEquals(new Reference('cache.system.flow_telemetry.inner'), $innerPools->getValues());
        static::assertContainsEquals(
            new Reference('cache.doctrine.orm.default.result.flow_telemetry.inner'),
            $innerPools->getValues(),
        );
    }

    public function test_the_real_pool_graph_compiles(): void
    {
        $container = CachePoolContainerMother::frameworkPoolGraph();
        $container->addCompilerPass(new CacheTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->compile();

        static::assertSame(TraceableCacheAdapter::class, $container->getDefinition('cache.system')->getClass());
        static::assertSame(
            TagAwareTraceableCacheAdapter::class,
            $container->getDefinition('app.redis_pool')->getClass(),
        );
    }
}
