<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\BuildFstabsPass;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\BuildFstabsPassContext;
use Flow\Filesystem\Telemetry\{FilesystemTelemetryConfig, FilesystemTelemetryOptions};
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\{Definition, Reference};

final class BuildFstabsPassTelemetryTest extends TestCase
{
    private BuildFstabsPassContext $context;

    protected function setUp() : void
    {
        $this->context = new BuildFstabsPassContext();
    }

    public function test_creates_telemetry_config_definition_when_enabled() : void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                    'telemetry' => [
                        'enabled' => true,
                        'telemetry_service_id' => 'app.telemetry',
                        'clock_service_id' => 'app.clock',
                        'options' => ['trace_streams' => false, 'collect_metrics' => true],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        self::assertTrue($container->hasDefinition('.flow_filesystem.telemetry_config.default'));
        $configDefinition = $container->getDefinition('.flow_filesystem.telemetry_config.default');
        self::assertSame(FilesystemTelemetryConfig::class, $configDefinition->getClass());

        $configArgs = $configDefinition->getArguments();
        self::assertInstanceOf(Reference::class, $configArgs[0]);
        self::assertSame('app.telemetry', (string) $configArgs[0]);
        self::assertInstanceOf(Reference::class, $configArgs[1]);
        self::assertSame('app.clock', (string) $configArgs[1]);
        self::assertInstanceOf(Definition::class, $configArgs[2]);
        self::assertSame(FilesystemTelemetryOptions::class, $configArgs[2]->getClass());
        self::assertSame([false, true], $configArgs[2]->getArguments());

        $fstabArgs = $container->getDefinition('.flow_filesystem.fstab.default')->getArguments();
        self::assertInstanceOf(Reference::class, $fstabArgs[3]);
        self::assertSame('.flow_filesystem.telemetry_config.default', (string) $fstabArgs[3]);
    }

    public function test_passes_null_telemetry_when_disabled() : void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                    'telemetry' => ['enabled' => false],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        $arguments = $container->getDefinition('.flow_filesystem.fstab.default')->getArguments();
        self::assertNull($arguments[3]);
    }

    public function test_throws_when_telemetry_enabled_without_clock_service_id() : void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                    'telemetry' => [
                        'enabled' => true,
                        'telemetry_service_id' => 'app.telemetry',
                    ],
                ],
            ],
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('clock_service_id');

        (new BuildFstabsPass())->process($container);
    }

    public function test_throws_when_telemetry_enabled_without_telemetry_service_id() : void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                    'telemetry' => [
                        'enabled' => true,
                        'clock_service_id' => 'app.clock',
                    ],
                ],
            ],
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('telemetry_service_id');

        (new BuildFstabsPass())->process($container);
    }
}
