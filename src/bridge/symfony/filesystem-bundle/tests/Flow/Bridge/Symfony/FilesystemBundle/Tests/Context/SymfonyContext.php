<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Context;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\TelemetryStubFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;
use LogicException;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\Filesystem\Filesystem;

use function Flow\Types\DSL\type_instance_of;
use function is_callable;

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
     * @param callable(ContainerBuilder): void $configurator
     */
    public function bootTracedMemoryFstab(callable $configurator): TestKernel
    {
        return $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($configurator): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
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
                            ],
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $configurator,
                ): void {
                    $container->setDefinition(
                        'app.telemetry',
                        (new Definition(Telemetry::class))->setFactory([TelemetryStubFactory::class, 'create']),
                    );
                    $container->setDefinition('app.clock', new Definition(SystemClock::class));
                    $configurator($container);
                });
            },
        ]);
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
        return type_instance_of($typeClass)->assert($this->getContainer()->get($serviceId));
    }

    public function shutdown(): void
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
    }
}
