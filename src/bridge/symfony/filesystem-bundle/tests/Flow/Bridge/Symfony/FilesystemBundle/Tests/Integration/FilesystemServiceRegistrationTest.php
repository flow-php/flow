<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\AliasFilesystemConsumer;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Filesystem\Filesystem;
use Symfony\Component\DependencyInjection\ContainerBuilder;

final class FilesystemServiceRegistrationTest extends KernelTestCase
{
    public function test_bare_alias_resolves_default_fstab_mount(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'memory' => ['type' => 'memory'],
                                'file' => ['type' => 'file'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $filesystem = $this->symfonyContext()->getService(Filesystem::class . ' $memory', Filesystem::class);

        static::assertSame('memory', $filesystem->mount()->protocol);
    }

    public function test_prefixed_alias_resolves_named_fstab_mount(): void
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
            },
        ]);

        $filesystem = $this->symfonyContext()->getService(Filesystem::class . ' $archiveFile', Filesystem::class);

        static::assertSame('file', $filesystem->mount()->protocol);
    }

    public function test_named_argument_injects_default_mount_into_consumer(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'memory' => ['type' => 'memory'],
                            ],
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.alias_consumer', AliasFilesystemConsumer::class)
                        ->setAutowired(true)
                        ->setPublic(true);
                });
            },
        ]);

        $consumer = $this->symfonyContext()->getService('test.alias_consumer', AliasFilesystemConsumer::class);

        static::assertSame('memory', $consumer->memory->mount()->protocol);
    }
}
