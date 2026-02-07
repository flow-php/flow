<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures;

use Flow\Bridge\Symfony\TelemetryBundle\FlowTelemetryBundle;
use Flow\Telemetry\Telemetry;
use Symfony\Component\Config\Loader\LoaderInterface;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\BundleInterface;
use Symfony\Component\HttpKernel\Kernel;

final class TestKernel extends Kernel
{
    /** @var array<class-string<BundleInterface>> */
    private array $testBundles = [];

    /** @var array<string> */
    private array $testConfigs = [];

    /** @var array<callable(ContainerBuilder): void> */
    private array $testContainerConfigurators = [];

    /** @var array<string, array<string, mixed>> */
    private array $testExtensionConfigs = [];

    private readonly string $testId;

    public function __construct(string $environment = 'test', bool $debug = true)
    {
        $this->testId = \bin2hex(\random_bytes(8));

        parent::__construct($environment, $debug);
    }

    /**
     * @param class-string<BundleInterface> $bundleClass
     */
    public function addTestBundle(string $bundleClass) : void
    {
        $this->testBundles[] = $bundleClass;
    }

    public function addTestConfig(string $configPath) : void
    {
        $this->testConfigs[] = $configPath;
    }

    /**
     * @param callable(ContainerBuilder): void $configurator
     */
    public function addTestContainerConfigurator(callable $configurator) : void
    {
        $this->testContainerConfigurators[] = $configurator;
    }

    /**
     * @param array<string, mixed> $config
     */
    public function addTestExtensionConfig(string $extension, array $config) : void
    {
        $this->testExtensionConfigs[$extension] = \array_merge(
            $this->testExtensionConfigs[$extension] ?? [],
            $config
        );
    }

    #[\Override]
    public function getCacheDir() : string
    {
        return __DIR__ . '/../../../../../../../var/flow_telemetry_bundle_test/' . $this->environment . '/cache';
    }

    #[\Override]
    public function getLogDir() : string
    {
        return __DIR__ . '/../../../../../../../var/flow_telemetry_bundle_test/' . $this->environment . '/log';
    }

    #[\Override]
    public function getProjectDir() : string
    {
        return __DIR__ . '/..';
    }

    public function registerBundles() : iterable
    {
        yield new FlowTelemetryBundle();

        foreach ($this->testBundles as $bundleClass) {
            yield new $bundleClass();
        }
    }

    public function registerContainerConfiguration(LoaderInterface $loader) : void
    {
        foreach ($this->testConfigs as $configPath) {
            $loader->load($configPath);
        }

        $loader->load(function (ContainerBuilder $container) : void {
            foreach ($this->testExtensionConfigs as $extension => $config) {
                $container->loadFromExtension($extension, $config);
            }

            foreach ($this->testContainerConfigurators as $configurator) {
                $configurator($container);
            }

            $container->setParameter('kernel.secret', 'test_secret_' . $this->testId);
        });
    }

    protected function build(ContainerBuilder $container) : void
    {
        parent::build($container);

        $container->addCompilerPass(new class implements CompilerPassInterface {
            public function process(ContainerBuilder $container) : void
            {
                foreach ($container->getDefinitions() as $id => $definition) {
                    if (\str_starts_with($id, 'flow.telemetry') || \str_ends_with($id, '.flow_telemetry') || \str_starts_with($id, 'test.')) {
                        $definition->setPublic(true);
                    }
                }

                foreach ($container->getAliases() as $id => $alias) {
                    if ($id === Telemetry::class || \str_starts_with($id, 'flow.telemetry')) {
                        $alias->setPublic(true);
                    }
                }
            }
        });
    }
}
