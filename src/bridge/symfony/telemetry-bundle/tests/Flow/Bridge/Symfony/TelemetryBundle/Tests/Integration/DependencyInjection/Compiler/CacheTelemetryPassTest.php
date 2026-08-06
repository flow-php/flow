<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\CacheTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\CompiledIdCollectorPass;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function restore_exception_handler;

/**
 * The unit test proves the pass against a replica of FrameworkBundle's pool graph. This one boots the
 * real thing, so a future change to how FrameworkBundle registers its pools cannot silently un-fix the
 * bug while the replica keeps passing.
 */
#[CoversClass(CacheTelemetryPass::class)]
final class CacheTelemetryPassTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_framework_cache_pools_are_traced_in_a_booted_kernel(): void
    {
        $collector = new CompiledIdCollectorPass();

        $this->bootKernel([
            'config' => function (TestKernel $kernel) use ($collector): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig(
                    'flow_telemetry',
                    $this->symfonyContext()->fullyPopulatedTelemetryConfig(true),
                );
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $collector,
                ): void {
                    $container->addCompilerPass($collector, PassConfig::TYPE_AFTER_REMOVING, -1024);
                });
            },
        ]);

        static::assertContains('cache.system.flow_telemetry', $collector->ids);
        static::assertContains('cache.validator.flow_telemetry', $collector->ids);
        static::assertContains('cache.serializer.flow_telemetry', $collector->ids);
        static::assertContains('cache.property_info.flow_telemetry', $collector->ids);
        static::assertContains('cache.app.flow_telemetry', $collector->ids);
    }

    public function test_abstract_pool_templates_are_not_traced_in_a_booted_kernel(): void
    {
        $collector = new CompiledIdCollectorPass();

        $this->bootKernel([
            'config' => function (TestKernel $kernel) use ($collector): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig(
                    'flow_telemetry',
                    $this->symfonyContext()->fullyPopulatedTelemetryConfig(true),
                );
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $collector,
                ): void {
                    $container->addCompilerPass($collector, PassConfig::TYPE_AFTER_REMOVING, -1024);
                });
            },
        ]);

        static::assertNotContains('cache.adapter.system.flow_telemetry', $collector->ids);
        static::assertNotContains('cache.adapter.filesystem.flow_telemetry', $collector->ids);
    }

    public function test_excluded_pools_are_not_traced_in_a_booted_kernel(): void
    {
        $collector = new CompiledIdCollectorPass();

        $this->bootKernel([
            'config' => function (TestKernel $kernel) use ($collector): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    ...$this->symfonyContext()->fullyPopulatedTelemetryConfig(true),
                    'instrumentation' => [
                        'cache' => [
                            'enabled' => true,
                            'exclude_pools' => ['cache.system', '/^cache\.validator.*/'],
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $collector,
                ): void {
                    $container->addCompilerPass($collector, PassConfig::TYPE_AFTER_REMOVING, -1024);
                });
            },
        ]);

        static::assertNotContains('cache.system.flow_telemetry', $collector->ids);
        static::assertNotContains('cache.validator.flow_telemetry', $collector->ids);
        static::assertContains('cache.app.flow_telemetry', $collector->ids);
    }
}
