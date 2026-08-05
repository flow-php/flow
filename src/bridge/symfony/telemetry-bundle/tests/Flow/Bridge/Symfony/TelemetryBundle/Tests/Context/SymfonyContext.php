<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Context;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Telemetry\Telemetry;
use LogicException;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\Filesystem\Filesystem;

use function dirname;
use function Flow\Types\DSL\type_instance_of;
use function is_callable;
use function str_starts_with;
use function sys_get_temp_dir;

final class SymfonyContext
{
    private ?TestKernel $kernel = null;

    /**
     * @param array{config?: callable(TestKernel): void} $options
     */
    public function bootKernel(array $options = []): TestKernel
    {
        if ($this->kernel !== null) {
            $this->shutdown();
        }

        $this->kernel = new TestKernel('test', false);

        if (isset($options['config']) && is_callable($options['config'])) {
            $options['config']($this->kernel);
        }

        $this->kernel->boot();

        return $this->kernel;
    }

    /**
     * @return array<string, mixed>
     */
    public function fullyPopulatedTelemetryConfig(bool $enabled): array
    {
        return [
            'enabled' => $enabled,
            'resource' => ['custom' => ['deployment.tenant' => '%env(default::FLOW_TELEMETRY_TEST_TENANT)%']],
            'exporters' => [
                'memory' => ['memory' => null],
                'void' => ['void' => null],
                'otlp_http' => [
                    'enabled' => '%env(bool:FLOW_TELEMETRY_TEST_EXPORTERS_ENABLED)%',
                    'otlp' => [
                        'transport' => [
                            'endpoint' => '%env(default::FLOW_TELEMETRY_TEST_OTLP_ENDPOINT)%',
                        ],
                    ],
                ],
            ],
            'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
            'instrumentation' => [
                'http_kernel' => [
                    'enabled' => true,
                    'trace_controller_resolution' => true,
                    'trace_controller_arguments' => true,
                    'trace_controller_argument_resolvers' => true,
                ],
                'console' => ['enabled' => true],
                'messenger' => ['enabled' => true],
                'http_client' => ['enabled' => true],
                'psr18_client' => ['enabled' => true],
                'dbal' => ['enabled' => true],
                'cache' => ['enabled' => true],
            ],
            'tracers' => ['app' => []],
            'meters' => ['app' => []],
            'loggers' => ['app' => []],
        ];
    }

    public function getContainer(): ContainerInterface
    {
        if ($this->kernel === null) {
            throw new LogicException('Kernel has not been booted. Call bootKernel() first.');
        }

        return $this->kernel->getContainer();
    }

    public function getKernel(): TestKernel
    {
        if ($this->kernel === null) {
            throw new LogicException('Kernel has not been booted. Call bootKernel() first.');
        }

        return $this->kernel;
    }

    /**
     * @template T of object
     *
     * @param class-string<T> $typeClass
     *
     * @return T
     */
    public function getService(string $serviceId, string $typeClass): object
    {
        $service = $this->getContainer()->get($serviceId);

        return type_instance_of($typeClass)->assert($service);
    }

    public function makeFlowServicesPublic(ContainerBuilder $container): void
    {
        foreach ($container->getDefinitions() as $id => $definition) {
            if (str_starts_with($id, 'flow.telemetry')) {
                $definition->setPublic(true);
            }
        }

        foreach ($container->getAliases() as $id => $alias) {
            if ($id === Telemetry::class || str_starts_with($id, 'flow.telemetry')) {
                $alias->setPublic(true);
            }
        }
    }

    public function shutdown(): void
    {
        if ($this->kernel === null) {
            return;
        }

        $runDir = dirname($this->kernel->getCacheDir());
        $environment = $this->kernel->getEnvironment();

        $this->kernel->shutdown();
        $this->kernel = null;

        $filesystem = new Filesystem();

        if ($filesystem->exists($runDir)) {
            $filesystem->remove($runDir);
        }

        $defaultResourceCache = sys_get_temp_dir() . '/flow_telemetry_resource_' . $environment . '.cache';

        if ($filesystem->exists($defaultResourceCache)) {
            $filesystem->remove($defaultResourceCache);
        }
    }
}
