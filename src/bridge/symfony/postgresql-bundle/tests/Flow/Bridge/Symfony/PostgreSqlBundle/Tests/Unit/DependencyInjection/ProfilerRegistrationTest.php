<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\DependencyInjection;

use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context\ExtensionContext;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Bundle\WebProfilerBundle\WebProfilerBundle;

#[CoversClass(FlowPostgreSqlBundle::class)]
final class ProfilerRegistrationTest extends TestCase
{
    private ExtensionContext $context;

    protected function setUp(): void
    {
        $this->context = new ExtensionContext();
    }

    public function test_auto_enabled_when_web_profiler_registered_and_kernel_is_in_debug_mode(): void
    {
        $container = $this->context->load($this->config(), debug: true, bundles: [WebProfilerBundle::class]);

        static::assertTrue($container->hasDefinition('flow.postgresql.profiler.query_recorder'));
        static::assertTrue($container->hasDefinition('flow.postgresql.default.client.profiler'));
    }

    public function test_auto_disabled_when_kernel_is_not_in_debug_mode(): void
    {
        $container = $this->context->load($this->config(), debug: false, bundles: [WebProfilerBundle::class]);

        static::assertFalse($container->hasDefinition('flow.postgresql.profiler.query_recorder'));
        static::assertFalse($container->hasDefinition('flow.postgresql.default.client.profiler'));
    }

    public function test_auto_disabled_when_web_profiler_bundle_is_absent_in_debug_mode(): void
    {
        $container = $this->context->load($this->config(), debug: true);

        static::assertFalse($container->hasDefinition('flow.postgresql.profiler.query_recorder'));
    }

    public function test_forcing_enabled_registers_without_debug_mode(): void
    {
        $container = $this->context->load(
            $this->config(['enabled' => true]),
            debug: false,
            bundles: [WebProfilerBundle::class],
        );

        static::assertTrue($container->hasDefinition('flow.postgresql.profiler.query_recorder'));
        static::assertTrue($container->hasDefinition('flow.postgresql.default.client.profiler'));
    }

    public function test_forcing_disabled_skips_registration_in_debug_mode(): void
    {
        $container = $this->context->load(
            $this->config(['enabled' => false]),
            debug: true,
            bundles: [WebProfilerBundle::class],
        );

        static::assertFalse($container->hasDefinition('flow.postgresql.profiler.query_recorder'));
    }

    public function test_max_query_length_is_wired_as_a_container_parameter(): void
    {
        $container = $this->context->load(
            $this->config(['max_query_length' => 250]),
            debug: true,
            bundles: [WebProfilerBundle::class],
        );

        static::assertSame(250, $container->getParameter('flow.postgresql.profiler.max_query_length'));
    }

    /**
     * @param array<string, mixed> $profiler
     *
     * @return array<string, mixed>
     */
    private function config(array $profiler = []): array
    {
        return [
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'profiler' => $profiler,
        ];
    }
}
