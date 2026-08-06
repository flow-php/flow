<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother;

use Flow\Telemetry\Telemetry;
use Symfony\Component\Cache\Adapter\AdapterInterface;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Adapter\RedisTagAwareAdapter;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\ContainerBuilder;

final class CachePoolContainerMother
{
    public static function frameworkPoolGraph(): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.cache.enabled', true);
        $container->register(Telemetry::class)->setSynthetic(true);

        $container->register('cache.adapter.system', AdapterInterface::class)->setAbstract(true)->addTag('cache.pool');
        $container
            ->register('cache.adapter.filesystem', FilesystemAdapter::class)
            ->setAbstract(true)
            ->addTag('cache.pool');

        $container->setDefinition(
            'cache.system',
            (new ChildDefinition('cache.adapter.system'))
                ->addTag('cache.pool')
                ->setPublic(true),
        );
        $container->setDefinition(
            'cache.validator',
            (new ChildDefinition('cache.system'))
                ->addTag('cache.pool')
                ->setPublic(true),
        );
        $container->setDefinition(
            'cache.app',
            (new ChildDefinition('cache.adapter.filesystem'))
                ->addTag('cache.pool')
                ->setPublic(true),
        );
        $container->setDefinition(
            'cache.doctrine.orm.default.result',
            (new ChildDefinition('cache.app'))
                ->addTag('cache.pool')
                ->setPublic(true),
        );

        $container->register('app.redis_pool', RedisTagAwareAdapter::class)->addTag('cache.pool')->setPublic(true);

        $container->setParameter('app.pool.class', FilesystemAdapter::class);
        $container->register('app.param_pool', '%app.pool.class%')->addTag('cache.pool')->setPublic(true);

        return $container;
    }
}
