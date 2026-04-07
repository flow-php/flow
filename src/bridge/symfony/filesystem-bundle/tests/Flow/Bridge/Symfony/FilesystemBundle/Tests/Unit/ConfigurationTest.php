<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit;

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

    public function test_default_fstab_implicitly_resolves_to_fstab_named_default() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => [],
                    ],
                ],
                'extra' => [
                    'filesystems' => [
                        'memory' => [],
                    ],
                ],
            ],
        ]);

        self::assertSame('default', $config['default_fstab']);
    }

    public function test_invalid_default_fstab_points_to_missing_fstab_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'default_fstab' => 'nope',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'file' => [],
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
                        'file' => [],
                    ],
                ],
                'secondary' => [
                    'filesystems' => [
                        'memory' => [],
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
                        '1bad' => [],
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
                        'bad/protocol' => [],
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
                        'file' => [],
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
                        'file' => [],
                    ],
                    'telemetry' => ['enabled' => true, 'clock_service_id' => 'x'],
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
                        'file' => [],
                    ],
                ],
            ],
        ]);

        self::assertSame('primary', $config['default_fstab']);
    }

    public function test_valid_hyphenated_protocol_keys_are_preserved_without_normalization() : void
    {
        $config = $this->context->processConfig([
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'aws-s3' => ['bucket' => 'b', 'client_service_id' => 'x'],
                        'azure-blob' => ['container' => 'c', 'client_service_id' => 'y'],
                    ],
                ],
            ],
        ]);

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
                        'file' => [],
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
                        'file' => [],
                    ],
                ],
                'secondary' => [
                    'filesystems' => [
                        'memory' => [],
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
                        'file' => [],
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
                            'file' => [],
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
