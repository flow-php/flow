<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FstabBuilder;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Telemetry\{FilesystemTelemetryConfig, FilesystemTelemetryOptions};
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference};

final class BuildFstabsPass implements CompilerPassInterface
{
    public const string CONFIG_PARAMETER = 'flow_filesystem.config';

    public const string FSTAB_SERVICE_PREFIX = '.flow_filesystem.fstab.';

    public const string TELEMETRY_CONFIG_SERVICE_PREFIX = '.flow_filesystem.telemetry_config.';

    public function process(ContainerBuilder $container) : void
    {
        if (!$container->hasParameter(self::CONFIG_PARAMETER)) {
            return;
        }

        /** @var array{default_fstab: null|string, fstabs: array<string, array{filesystems: array<string, array<string, mixed>>, telemetry?: array<string, mixed>}>} $config */
        $config = $container->getParameter(self::CONFIG_PARAMETER);

        $fstabs = $config['fstabs'];
        $defaultFstab = $config['default_fstab'];

        if ($defaultFstab !== null && !\array_key_exists($defaultFstab, $fstabs)) {
            throw new LogicException(\sprintf(
                'flow_filesystem: default_fstab "%s" does not match any configured fstab. Available: [%s].',
                $defaultFstab,
                \implode(', ', \array_keys($fstabs)),
            ));
        }

        $availableProtocols = $this->collectAvailableProtocols($container);

        foreach ($fstabs as $fstabName => $fstabConfig) {
            $entries = [];

            foreach ($fstabConfig['filesystems'] as $protocolName => $entry) {
                if (!\array_key_exists($protocolName, $availableProtocols)) {
                    throw new LogicException(\sprintf(
                        'Fstab "%s" protocol "%s": no filesystem factory registered for this protocol. Available protocols: [%s].',
                        $fstabName,
                        $protocolName,
                        \implode(', ', \array_keys($availableProtocols)),
                    ));
                }

                $entries[$protocolName] = $entry;
            }

            $telemetryReference = $this->buildTelemetryConfigReference($container, $fstabName, $fstabConfig['telemetry'] ?? []);

            $definition = new Definition(FilesystemTable::class);
            $definition->setFactory([FstabBuilder::class, 'build']);
            $definition->setArguments([
                new Reference(RegisterFilesystemFactoriesPass::REGISTRY_SERVICE_ID),
                $fstabName,
                $entries,
                $telemetryReference,
            ]);
            $definition->setPublic(false);

            $serviceId = self::FSTAB_SERVICE_PREFIX . $fstabName;
            $container->setDefinition($serviceId, $definition);

            $aliasId = FilesystemTable::class . ' $' . $this->camelCase($fstabName) . 'Fstab';
            $container->setAlias($aliasId, $serviceId)->setPublic(true);
        }

        if ($defaultFstab !== null) {
            $container->setAlias(FilesystemTable::class, self::FSTAB_SERVICE_PREFIX . $defaultFstab)->setPublic(true);
        }
    }

    /**
     * @param array<string, mixed> $telemetry
     */
    private function buildTelemetryConfigReference(ContainerBuilder $container, string $fstabName, array $telemetry) : ?Reference
    {
        if (($telemetry['enabled'] ?? false) !== true) {
            return null;
        }

        $telemetryServiceId = $telemetry['telemetry_service_id'] ?? null;
        $clockServiceId = $telemetry['clock_service_id'] ?? null;

        if (!\is_string($telemetryServiceId) || $telemetryServiceId === '') {
            throw new LogicException(\sprintf('Fstab "%s" telemetry: telemetry_service_id must be a non-empty string when enabled.', $fstabName));
        }

        if (!\is_string($clockServiceId) || $clockServiceId === '') {
            throw new LogicException(\sprintf('Fstab "%s" telemetry: clock_service_id must be a non-empty string when enabled.', $fstabName));
        }

        /** @var array<string, mixed> $optionsConfig */
        $optionsConfig = $telemetry['options'] ?? [];
        $traceStreams = (bool) ($optionsConfig['trace_streams'] ?? true);
        $collectMetrics = (bool) ($optionsConfig['collect_metrics'] ?? true);

        $optionsDefinition = new Definition(FilesystemTelemetryOptions::class);
        $optionsDefinition->setArguments([$traceStreams, $collectMetrics]);
        $optionsDefinition->setPublic(false);

        $configDefinition = new Definition(FilesystemTelemetryConfig::class);
        $configDefinition->setArguments([
            new Reference($telemetryServiceId),
            new Reference($clockServiceId),
            $optionsDefinition,
        ]);
        $configDefinition->setPublic(false);

        $configServiceId = self::TELEMETRY_CONFIG_SERVICE_PREFIX . $fstabName;
        $container->setDefinition($configServiceId, $configDefinition);

        return new Reference($configServiceId);
    }

    private function camelCase(string $name) : string
    {
        return \lcfirst(\str_replace(' ', '', \ucwords(\str_replace('_', ' ', $name))));
    }

    /**
     * @return array<string, string>
     */
    private function collectAvailableProtocols(ContainerBuilder $container) : array
    {
        $protocols = [];

        foreach ($container->findTaggedServiceIds(RegisterFilesystemFactoriesPass::TAG) as $serviceId => $tags) {
            foreach ($tags as $tag) {
                if (\array_key_exists('protocol', $tag) && \is_string($tag['protocol']) && $tag['protocol'] !== '') {
                    $protocols[$tag['protocol']] = $serviceId;
                }
            }
        }

        return $protocols;
    }
}
