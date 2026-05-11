<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\FilesystemCache\FlowFilesystemCacheAdapter;
use Symfony\Component\Cache\Marshaller\DefaultMarshaller;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

final class CachePoolRegistrationTest extends KernelTestCase
{
    public function test_cache_pool_resolves_filesystem_through_default_fstab(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => ['filesystems' => ['file' => ['type' => 'file']]],
                    ],
                    'cache' => [
                        'pools' => [
                            'app' => [
                                'filesystem' => 'file',
                                'path' => \sys_get_temp_dir() . '/flow-fs-cache-bundle-test',
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(
            FlowFilesystemCacheAdapter::class,
            $this->getContainer()->get('flow.filesystem.cache.pool.app'),
        );
    }

    public function test_cache_pool_with_marshaller_service_id_injects_marshaller(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => ['filesystems' => ['file' => ['type' => 'file']]],
                    ],
                    'cache' => [
                        'pools' => [
                            'app' => [
                                'filesystem' => 'file',
                                'path' => \sys_get_temp_dir() . '/flow-fs-cache-bundle-test-marshaller',
                                'marshaller_service_id' => 'test.marshaller',
                            ],
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setDefinition(
                        'test.marshaller',
                        (new Definition(DefaultMarshaller::class))->setPublic(true),
                    );
                });
            },
        ]);

        $container = $this->getContainer();
        $adapter = $container->get('flow.filesystem.cache.pool.app');
        static::assertInstanceOf(FlowFilesystemCacheAdapter::class, $adapter);

        $marshaller = (new \ReflectionObject($adapter))
            ->getProperty('marshaller')
            ->getValue($adapter);
        static::assertSame($container->get('test.marshaller'), $marshaller);
    }

    public function test_cache_pool_with_named_fstab_resolves_through_that_fstab(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'default_fstab' => 'primary',
                    'fstabs' => [
                        'primary' => ['filesystems' => ['file' => ['type' => 'file']]],
                        'cache_only' => ['filesystems' => ['scratch' => ['type' => 'memory']]],
                    ],
                    'cache' => [
                        'pools' => [
                            'app' => [
                                'fstab' => 'cache_only',
                                'filesystem' => 'scratch',
                                'path' => 'memory://cache/app',
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(
            FlowFilesystemCacheAdapter::class,
            $this->getContainer()->get('flow.filesystem.cache.pool.app'),
        );
    }

    public function test_no_cache_pool_services_when_cache_section_is_omitted(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => ['filesystems' => ['file' => ['type' => 'file']]],
                    ],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.filesystem.cache.pool.app'));
    }
}
