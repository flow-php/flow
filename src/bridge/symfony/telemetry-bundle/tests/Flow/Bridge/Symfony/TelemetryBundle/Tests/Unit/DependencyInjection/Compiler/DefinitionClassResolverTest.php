<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\DefinitionClassResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Psr18\MockPsr18Client;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Cache\Adapter\AdapterInterface;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

#[CoversClass(DefinitionClassResolver::class)]
final class DefinitionClassResolverTest extends TestCase
{
    public function test_resolves_a_directly_declared_class(): void
    {
        $container = new ContainerBuilder();

        static::assertSame(
            MockPsr18Client::class,
            (new DefinitionClassResolver($container))->resolve($container->register(
                'app.client',
                MockPsr18Client::class,
            )),
        );
    }

    public function test_resolves_a_class_from_a_direct_parent(): void
    {
        $container = new ContainerBuilder();
        $container->register('app.base_client', MockPsr18Client::class)->setAbstract(true);
        $container->setDefinition('app.client', new ChildDefinition('app.base_client'));

        static::assertSame(
            MockPsr18Client::class,
            (new DefinitionClassResolver($container))->resolve($container->getDefinition('app.client')),
        );
    }

    public function test_resolves_a_class_through_a_multi_level_parent_chain(): void
    {
        $container = new ContainerBuilder();
        $container->register('app.grandparent_client', MockPsr18Client::class)->setAbstract(true);
        $container->setDefinition(
            'app.parent_client',
            (new ChildDefinition('app.grandparent_client'))->setAbstract(true),
        );
        $container->setDefinition('app.client', new ChildDefinition('app.parent_client'));

        static::assertSame(
            MockPsr18Client::class,
            (new DefinitionClassResolver($container))->resolve($container->getDefinition('app.client')),
        );
    }

    public function test_resolves_a_class_declared_as_a_parameter(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('app.client.class', MockPsr18Client::class);

        static::assertSame(
            MockPsr18Client::class,
            (new DefinitionClassResolver($container))->resolve($container->register(
                'app.client',
                '%app.client.class%',
            )),
        );
    }

    public function test_returns_null_for_a_circular_parent_chain(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('app.first', new ChildDefinition('app.second'));
        $container->setDefinition('app.second', new ChildDefinition('app.first'));

        static::assertNull((new DefinitionClassResolver($container))->resolve($container->getDefinition('app.first')));
    }

    public function test_returns_null_for_a_definition_that_is_its_own_parent(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('app.client', new ChildDefinition('app.client'));

        static::assertNull((new DefinitionClassResolver($container))->resolve($container->getDefinition('app.client')));
    }

    public function test_returns_null_when_the_parent_service_is_missing(): void
    {
        $container = new ContainerBuilder();

        static::assertNull((new DefinitionClassResolver($container))->resolve(
            new ChildDefinition('app.does_not_exist'),
        ));
    }

    public function test_returns_null_when_the_class_does_not_exist(): void
    {
        $container = new ContainerBuilder();

        static::assertNull((new DefinitionClassResolver($container))->resolve($container->register(
            'app.client',
            'Flow\Bridge\Symfony\TelemetryBundle\Tests\NoSuchClass',
        )));
    }

    public function test_returns_null_when_the_class_parameter_does_not_exist(): void
    {
        $container = new ContainerBuilder();

        static::assertNull((new DefinitionClassResolver($container))->resolve($container->register(
            'app.client',
            '%app.absent.class%',
        )));
    }

    public function test_returns_null_when_the_class_parameter_is_not_a_string(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('app.client.class', [MockPsr18Client::class]);

        static::assertNull((new DefinitionClassResolver($container))->resolve($container->register(
            'app.client',
            '%app.client.class%',
        )));
    }

    public function test_resolves_an_interface_declared_as_a_class(): void
    {
        $container = new ContainerBuilder();

        static::assertSame(
            AdapterInterface::class,
            (new DefinitionClassResolver($container))->resolve($container->register(
                'cache.adapter.system',
                AdapterInterface::class,
            )),
        );
    }

    public function test_returns_null_for_a_definition_without_a_class(): void
    {
        static::assertNull((new DefinitionClassResolver(new ContainerBuilder()))->resolve(new Definition()));
    }
}
