<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Context;

use function Flow\Types\DSL\type_instance_of;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\{ContainerBuilder, ContainerInterface};

use Symfony\Component\Filesystem\Filesystem;

final class SymfonyContext
{
    private ?TestKernel $kernel = null;

    /**
     * @param array{config?: callable(TestKernel): void} $options
     */
    public function bootKernel(array $options = []) : TestKernel
    {
        if ($this->kernel !== null) {
            $this->shutdown();
        }

        $this->kernel = new TestKernel('test', false);

        if (isset($options['config']) && \is_callable($options['config'])) {
            $options['config']($this->kernel);
        }

        $this->kernel->boot();

        return $this->kernel;
    }

    public function getContainer() : ContainerInterface
    {
        if ($this->kernel === null) {
            throw new \LogicException('Kernel has not been booted. Call bootKernel() first.');
        }

        return $this->kernel->getContainer();
    }

    public function getKernel() : TestKernel
    {
        if ($this->kernel === null) {
            throw new \LogicException('Kernel has not been booted. Call bootKernel() first.');
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
    public function getService(string $serviceId, string $typeClass) : object
    {
        $service = $this->getContainer()->get($serviceId);

        return type_instance_of($typeClass)->assert($service);
    }

    public function makeFlowServicesPublic(ContainerBuilder $container) : void
    {
        foreach ($container->getDefinitions() as $id => $definition) {
            if (\str_starts_with($id, 'flow.telemetry')) {
                $definition->setPublic(true);
            }
        }

        foreach ($container->getAliases() as $id => $alias) {
            if ($id === Telemetry::class || \str_starts_with($id, 'flow.telemetry')) {
                $alias->setPublic(true);
            }
        }
    }

    public function shutdown() : void
    {
        if ($this->kernel === null) {
            return;
        }

        $cacheDir = $this->kernel->getCacheDir();
        $logDir = $this->kernel->getLogDir();

        $this->kernel->shutdown();
        $this->kernel = null;

        $filesystem = new Filesystem();

        if ($filesystem->exists($cacheDir)) {
            $filesystem->remove($cacheDir);
        }

        if ($filesystem->exists($logDir)) {
            $filesystem->remove($logDir);
        }

        $defaultResourceCache = \sys_get_temp_dir() . '/flow_telemetry_resource.cache';

        if ($filesystem->exists($defaultResourceCache)) {
            $filesystem->remove($defaultResourceCache);
        }
    }
}
