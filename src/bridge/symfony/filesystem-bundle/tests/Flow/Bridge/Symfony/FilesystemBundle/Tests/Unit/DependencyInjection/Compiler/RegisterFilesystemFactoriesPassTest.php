<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\RegisterFilesystemFactoriesPass;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\{MemoryFilesystemFactory, NativeLocalFilesystemFactory};
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference};

final class RegisterFilesystemFactoriesPassTest extends TestCase
{
    public function test_does_nothing_when_registry_definition_absent() : void
    {
        $container = new ContainerBuilder();

        (new RegisterFilesystemFactoriesPass())->process($container);

        self::assertFalse($container->hasDefinition('.flow.filesystem.factory_registry'));
    }

    public function test_injects_tagged_factories_as_references() : void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('.flow.filesystem.factory_registry', (new Definition(FilesystemFactoryRegistry::class))->setArgument(0, []));

        $memory = new Definition(MemoryFilesystemFactory::class);
        $memory->addTag('flow.filesystem.factory', ['type' => 'memory']);
        $container->setDefinition('.flow.filesystem.factory.memory', $memory);

        $native = new Definition(NativeLocalFilesystemFactory::class);
        $native->addTag('flow.filesystem.factory', ['type' => 'file']);
        $container->setDefinition('.flow.filesystem.factory.file', $native);

        (new RegisterFilesystemFactoriesPass())->process($container);

        $argument = $container->getDefinition('.flow.filesystem.factory_registry')->getArgument(0);
        self::assertIsArray($argument);
        self::assertCount(2, $argument);
        self::assertContainsOnlyInstancesOf(Reference::class, $argument);

        $ids = \array_map(static fn (Reference $r) : string => (string) $r, $argument);
        \sort($ids);
        self::assertSame(['.flow.filesystem.factory.file', '.flow.filesystem.factory.memory'], $ids);
    }

    public function test_leaves_registry_empty_when_no_tagged_services() : void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('.flow.filesystem.factory_registry', (new Definition(FilesystemFactoryRegistry::class))->setArgument(0, []));

        (new RegisterFilesystemFactoriesPass())->process($container);

        self::assertSame([], $container->getDefinition('.flow.filesystem.factory_registry')->getArgument(0));
    }

    public function test_throws_on_duplicate_type_across_services() : void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('.flow.filesystem.factory_registry', (new Definition(FilesystemFactoryRegistry::class))->setArgument(0, []));

        $a = new Definition(MemoryFilesystemFactory::class);
        $a->addTag('flow.filesystem.factory', ['type' => 'memory']);
        $container->setDefinition('.flow.filesystem.factory.a', $a);

        $b = new Definition(MemoryFilesystemFactory::class);
        $b->addTag('flow.filesystem.factory', ['type' => 'memory']);
        $container->setDefinition('.flow.filesystem.factory.b', $b);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Duplicate filesystem factory for type "memory"');

        (new RegisterFilesystemFactoriesPass())->process($container);
    }

    public function test_throws_on_tag_without_type_attribute() : void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('.flow.filesystem.factory_registry', (new Definition(FilesystemFactoryRegistry::class))->setArgument(0, []));

        $memory = new Definition(MemoryFilesystemFactory::class);
        $memory->addTag('flow.filesystem.factory');
        $container->setDefinition('.flow.filesystem.factory.memory', $memory);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('missing a non-empty "type" attribute');

        (new RegisterFilesystemFactoriesPass())->process($container);
    }
}
