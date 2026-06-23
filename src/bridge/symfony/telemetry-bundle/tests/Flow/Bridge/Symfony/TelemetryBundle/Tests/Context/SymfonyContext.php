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

        $this->kernel->shutdown();
        $this->kernel = null;

        $filesystem = new Filesystem();

        if ($filesystem->exists($runDir)) {
            $filesystem->remove($runDir);
        }

        $defaultResourceCache = sys_get_temp_dir() . '/flow_telemetry_resource.cache';

        if ($filesystem->exists($defaultResourceCache)) {
            $filesystem->remove($defaultResourceCache);
        }
    }
}
