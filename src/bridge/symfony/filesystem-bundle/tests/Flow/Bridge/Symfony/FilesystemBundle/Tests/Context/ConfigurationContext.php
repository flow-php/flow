<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Context;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function is_array;

final readonly class ConfigurationContext
{
    private SymfonyContext $symfony;

    public function __construct()
    {
        $this->symfony = new SymfonyContext();
    }

    /**
     * @param array<string, mixed> $flowFilesystemConfig
     * @param null|callable(ContainerBuilder): void $containerConfigurator
     *
     * @return array<array-key, mixed>
     */
    public function processConfig(array $flowFilesystemConfig, ?callable $containerConfigurator = null): array
    {
        $kernel = $this->symfony->bootKernel([
            'config' => static function (TestKernel $kernel) use ($flowFilesystemConfig, $containerConfigurator): void {
                $kernel->addTestExtensionConfig('flow_filesystem', $flowFilesystemConfig);

                if ($containerConfigurator !== null) {
                    $kernel->addTestContainerConfigurator($containerConfigurator);
                }
            },
        ]);

        $config = $kernel->getContainer()->getParameter('flow.filesystem.config');

        if (!is_array($config)) {
            return [];
        }

        return $config;
    }

    public function shutdown(): void
    {
        $this->symfony->shutdown();
    }
}
