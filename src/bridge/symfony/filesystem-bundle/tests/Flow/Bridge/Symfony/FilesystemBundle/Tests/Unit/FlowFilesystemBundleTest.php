<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\FlowFilesystemBundle;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\{ChildDefinition, ContainerBuilder};

final class FlowFilesystemBundleTest extends TestCase
{
    public function test_build_registers_as_filesystem_factory_attribute_for_autoconfiguration() : void
    {
        $container = new ContainerBuilder();
        (new FlowFilesystemBundle())->build($container);

        $childDefinition = new ChildDefinition('parent');

        if (\method_exists($container, 'getAttributeAutoconfigurators')) {
            $autoconfigured = $container->getAttributeAutoconfigurators();
            self::assertArrayHasKey(AsFilesystemFactory::class, $autoconfigured);

            foreach ($autoconfigured[AsFilesystemFactory::class] as $configurator) {
                $configurator($childDefinition, new AsFilesystemFactory(protocol: 'my-fs'), new \ReflectionClass(\stdClass::class));
            }
        } else {
            $autoconfigured = $container->getAutoconfiguredAttributes();
            self::assertArrayHasKey(AsFilesystemFactory::class, $autoconfigured);
            $autoconfigured[AsFilesystemFactory::class]($childDefinition, new AsFilesystemFactory(protocol: 'my-fs'), new \ReflectionClass(\stdClass::class));
        }

        $tags = $childDefinition->getTag('flow_filesystem.factory');
        self::assertCount(1, $tags);
        self::assertSame(['protocol' => 'my-fs'], $tags[0]);
    }

    public function test_bundle_alias_resolves_to_flow_filesystem() : void
    {
        $extension = (new FlowFilesystemBundle())->getContainerExtension();

        self::assertNotNull($extension);
        self::assertSame('flow_filesystem', $extension->getAlias());
    }

    public function test_load_extension_compiles_with_empty_config() : void
    {
        $bundle = new FlowFilesystemBundle();
        $extension = $bundle->getContainerExtension();
        self::assertNotNull($extension);

        $container = new ContainerBuilder();
        $container->setParameter('kernel.debug', false);
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.build_dir', \sys_get_temp_dir());
        $container->registerExtension($extension);
        $container->loadFromExtension($extension->getAlias(), [
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => [],
                    ],
                ],
            ],
        ]);
        $container->compile();

        self::assertTrue($container->hasExtension('flow_filesystem'));
    }
}
