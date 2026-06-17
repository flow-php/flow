<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\AttributeFilesystemConsumer;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Symfony\Component\DependencyInjection\ContainerBuilder;

final class AsFilesystemAttributeTest extends KernelTestCase
{
    public function test_attribute_injects_default_and_explicit_fstab_mounts(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'default_fstab' => 'primary',
                    'fstabs' => [
                        'primary' => [
                            'filesystems' => [
                                'memory' => ['type' => 'memory'],
                            ],
                        ],
                        'archive' => [
                            'filesystems' => [
                                'file' => ['type' => 'file'],
                            ],
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.attribute_consumer', AttributeFilesystemConsumer::class)
                        ->setAutoconfigured(true)
                        ->setAutowired(true)
                        ->setPublic(true);
                });
            },
        ]);

        $consumer = $this->symfonyContext()->getService('test.attribute_consumer', AttributeFilesystemConsumer::class);

        static::assertSame('memory', $consumer->primary->mount()->protocol);
        static::assertSame('file', $consumer->cold->mount()->protocol);
    }
}
