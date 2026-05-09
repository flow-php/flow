<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit;

use AsyncAws\S3\S3Client;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\ConfigurationContext;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\TelemetryStubFactory;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition};

final class ConfigurationTest extends TestCase
{
    private ConfigurationContext $context;

    protected function setUp() : void
    {
        $this->context = new ConfigurationContext();
    }

    protected function tearDown() : void
    {
        $this->context->shutdown();
    }

    public function test_cache_pool_default_lifetime_default_is_zero() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['filesystem' => 'file', 'path' => '/cache'],
                ],
            ],
        ]);

        self::assertSame(0, $config['cache']['pools']['app']['default_lifetime']);
    }

    public function test_cache_pool_fstab_default_is_null() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['filesystem' => 'file', 'path' => '/cache'],
                ],
            ],
        ]);

        self::assertNull($config['cache']['pools']['app']['fstab']);
    }

    public function test_cache_pool_marshaller_service_id_default_is_null() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['filesystem' => 'file', 'path' => '/cache'],
                ],
            ],
        ]);

        self::assertNull($config['cache']['pools']['app']['marshaller_service_id']);
    }

    public function test_cache_pool_namespace_default_is_empty_string() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['filesystem' => 'file', 'path' => '/cache'],
                ],
            ],
        ]);

        self::assertSame('', $config['cache']['pools']['app']['namespace']);
    }

    public function test_cache_pool_requires_filesystem() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['path' => '/cache'],
                ],
            ],
        ]);
    }

    public function test_cache_pool_requires_path() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['filesystem' => 'file'],
                ],
            ],
        ]);
    }

    public function test_cache_pool_unknown_filesystem_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['filesystem' => 'no-such-mount', 'path' => '/cache'],
                ],
            ],
        ]);
    }

    public function test_cache_pool_unknown_fstab_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['fstab' => 'no-such-fstab', 'filesystem' => 'file', 'path' => '/cache'],
                ],
            ],
        ]);
    }

    public function test_cache_pools_can_be_configured_with_minimum_fields() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
            'cache' => [
                'pools' => [
                    'app' => [
                        'filesystem' => 'file',
                        'path' => '/cache',
                    ],
                ],
            ],
        ]);

        self::assertSame('file', $config['cache']['pools']['app']['filesystem']);
        self::assertSame('/cache', $config['cache']['pools']['app']['path']);
    }

    public function test_default_fstab_implicitly_resolves_to_fstab_named_default() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
                'extra' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        self::assertSame('default', $config['default_fstab']);
    }

    public function test_empty_mount_type_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => ''],
                    ],
                ],
            ],
        ]);
    }

    public function test_invalid_default_fstab_points_to_missing_fstab_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'default_fstab' => 'nope',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);
    }

    public function test_invalid_empty_fstabs_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig(['fstabs' => []]);
    }

    public function test_invalid_fstab_with_zero_filesystems_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => ['filesystems' => []],
            ],
        ]);
    }

    public function test_invalid_multi_fstab_without_explicit_default_and_without_default_name_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'primary' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
                'secondary' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);
    }

    public function test_invalid_protocol_name_regex_throws_numeric_start() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        '1bad' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);
    }

    public function test_invalid_protocol_name_regex_throws_special_chars() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'bad/protocol' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);
    }

    public function test_invalid_telemetry_enabled_without_clock_service_id_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                    'telemetry' => ['enabled' => true, 'telemetry_service_id' => 'x'],
                ],
            ],
        ]);
    }

    public function test_invalid_telemetry_enabled_without_telemetry_service_id_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                    'telemetry' => ['enabled' => true, 'clock_service_id' => 'x'],
                ],
            ],
        ]);
    }

    public function test_missing_mount_type_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => [],
                    ],
                ],
            ],
        ]);
    }

    public function test_single_fstab_without_explicit_default_resolves_to_only_fstab() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'primary' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);

        self::assertSame('primary', $config['default_fstab']);
    }

    public function test_valid_hyphenated_mount_names_are_preserved_without_normalization() : void
    {
        $config = $this->context->processConfig(
            [
                'fstabs' => [
                    'default' => [
                        'filesystems' => [
                            'aws-s3' => ['type' => 'aws_s3', 'bucket' => 'b', 'client_service_id' => 'x'],
                            'azure-blob' => ['type' => 'azure_blob', 'container' => 'c', 'client_service_id' => 'y'],
                        ],
                    ],
                ],
            ],
            static function (ContainerBuilder $container) : void {
                $container->setDefinition('x', (new Definition(S3Client::class))
                    ->setArguments([['accessKeyId' => 'k', 'accessKeySecret' => 's', 'region' => 'us-east-1']])
                    ->setPublic(false));
                $container->setDefinition('y', (new Definition(BlobServiceInterface::class))
                    ->setSynthetic(true)
                    ->setPublic(false));
                $container->set('y', self::createStub(BlobServiceInterface::class));
            },
        );

        self::assertArrayHasKey('aws-s3', $config['fstabs']['default']['filesystems']);
        self::assertArrayHasKey('azure-blob', $config['fstabs']['default']['filesystems']);
        self::assertArrayNotHasKey('aws_s3', $config['fstabs']['default']['filesystems']);
        self::assertArrayNotHasKey('azure_blob', $config['fstabs']['default']['filesystems']);
    }

    public function test_valid_minimal_single_fstab() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);

        self::assertSame('default', $config['default_fstab']);
        self::assertArrayHasKey('file', $config['fstabs']['default']['filesystems']);
    }

    public function test_valid_multi_fstab_with_explicit_default() : void
    {
        $config = $this->context->processConfig([
            'default_fstab' => 'secondary',
            'fstabs' => [
                'primary' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
                'secondary' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        self::assertSame('secondary', $config['default_fstab']);
        self::assertArrayHasKey('primary', $config['fstabs']);
        self::assertArrayHasKey('secondary', $config['fstabs']);
    }

    public function test_valid_telemetry_disabled_by_default() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);

        self::assertFalse($config['fstabs']['default']['telemetry']['enabled']);
    }

    public function test_valid_telemetry_fully_configured() : void
    {
        $config = $this->context->processConfig(
            [
                'fstabs' => [
                    'default' => [
                        'filesystems' => [
                            'file' => ['type' => 'file'],
                        ],
                        'telemetry' => [
                            'enabled' => true,
                            'telemetry_service_id' => 'app.telemetry',
                            'clock_service_id' => 'app.clock',
                            'options' => ['trace_streams' => false, 'collect_metrics' => false],
                        ],
                    ],
                ],
            ],
            static function (ContainerBuilder $container) : void {
                $container->setDefinition(
                    'app.telemetry',
                    (new Definition(Telemetry::class))
                        ->setFactory([TelemetryStubFactory::class, 'create'])
                        ->setPublic(true),
                );
                $container->setDefinition('app.clock', (new Definition(SystemClock::class))->setPublic(true));
            },
        );

        self::assertTrue($config['fstabs']['default']['telemetry']['enabled']);
        self::assertSame('app.telemetry', $config['fstabs']['default']['telemetry']['telemetry_service_id']);
        self::assertFalse($config['fstabs']['default']['telemetry']['options']['trace_streams']);
    }
}
