<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\RegisterFilesystemFactoriesPass;
use Flow\Bridge\Symfony\FilesystemBundle\FlowFilesystemBundle;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\AutoconfiguredStubFilesystemFactory;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;

final class FlowFilesystemBundleTest extends TestCase
{
    public function test_build_registers_as_filesystem_factory_attribute_for_autoconfiguration() : void
    {
        $container = new ContainerBuilder();
        (new FlowFilesystemBundle())->build($container);
        $container
            ->register(AutoconfiguredStubFilesystemFactory::class, AutoconfiguredStubFilesystemFactory::class)
            ->setAutoconfigured(true)
            ->setPublic(true);

        $container->compile();

        self::assertSame(
            [['type' => 'file']],
            $container->getDefinition(AutoconfiguredStubFilesystemFactory::class)->getTag(RegisterFilesystemFactoriesPass::TAG),
        );
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
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);
        $container->compile();

        self::assertTrue($container->hasExtension('flow_filesystem'));
    }
}
