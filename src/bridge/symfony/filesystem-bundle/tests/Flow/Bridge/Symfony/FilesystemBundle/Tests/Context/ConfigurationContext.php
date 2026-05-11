<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Context;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Symfony\Component\DependencyInjection\ContainerBuilder;

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
     * @return array{default_fstab: string, fstabs: array<string, array{filesystems: array<string, array<string, mixed>>, telemetry: array{enabled: bool, telemetry_service_id: null|string, clock_service_id: null|string, options: array{trace_streams: bool, collect_metrics: bool}}}>, cache: array{pools: array<string, array{fstab: null|string, filesystem: string, path: string, namespace: string, default_lifetime: int, marshaller_service_id: null|string}>}}
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

        /** @phpstan-ignore return.type */
        return $kernel->getContainer()->getParameter('flow.filesystem.config');
    }

    public function shutdown(): void
    {
        $this->symfony->shutdown();
    }
}
