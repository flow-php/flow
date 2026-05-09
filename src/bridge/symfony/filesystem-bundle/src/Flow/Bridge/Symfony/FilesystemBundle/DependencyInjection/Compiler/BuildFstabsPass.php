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
    public const string CONFIG_PARAMETER = 'flow.filesystem.config';

    public const string FSTAB_SERVICE_PREFIX = '.flow.filesystem.fstab.';

    public const string TELEMETRY_CONFIG_SERVICE_PREFIX = '.flow.filesystem.telemetry_config.';

    public function process(ContainerBuilder $container) : void
    {
        if (!$container->hasParameter(self::CONFIG_PARAMETER)) {
            return;
        }

        /** @var array{default_fstab: null|string, fstabs: array<string, array{filesystems: array<string, array<string, mixed>&array{type: string}>, telemetry?: array<string, mixed>}>} $config */
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

        $availableTypes = $this->collectAvailableTypes($container);

        foreach ($fstabs as $fstabName => $fstabConfig) {
            $resolvedFilesystems = [];

            foreach ($fstabConfig['filesystems'] as $mountName => $entry) {
                if (!\array_key_exists($entry['type'], $availableTypes)) {
                    throw new LogicException(\sprintf(
                        'Fstab "%s" mount "%s": no filesystem factory registered for type "%s". Available types: [%s].',
                        $fstabName,
                        $mountName,
                        $entry['type'],
                        \implode(', ', \array_keys($availableTypes)),
                    ));
                }

                $resolvedFilesystems[$mountName] = $this->resolveServiceReferences($entry);
            }

            $telemetryReference = $this->buildTelemetryConfigReference($container, $fstabName, $fstabConfig['telemetry'] ?? []);

            $definition = new Definition(FilesystemTable::class);
            $definition->setFactory([FstabBuilder::class, 'build']);
            $definition->setArguments([
                new Reference(RegisterFilesystemFactoriesPass::REGISTRY_SERVICE_ID),
                $fstabName,
                $resolvedFilesystems,
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
    private function collectAvailableTypes(ContainerBuilder $container) : array
    {
        $types = [];

        foreach ($container->findTaggedServiceIds(RegisterFilesystemFactoriesPass::TAG) as $serviceId => $tags) {
            foreach ($tags as $tag) {
                if (\array_key_exists('type', $tag) && \is_string($tag['type']) && $tag['type'] !== '') {
                    $types[$tag['type']] = $serviceId;
                }
            }
        }

        return $types;
    }

    /**
     * @param array<string, mixed>&array{type: string} $entry
     *
     * @return array<string, mixed>&array{type: string}
     */
    private function resolveAwsS3References(array $entry) : array
    {
        if (\array_key_exists('client_service_id', $entry) && \is_string($entry['client_service_id']) && $entry['client_service_id'] !== '') {
            $entry['client'] = new Reference($entry['client_service_id']);
            unset($entry['client_service_id']);
        }

        if (\array_key_exists('client', $entry) && \is_array($entry['client'])) {
            $client = $entry['client'];

            if (\array_key_exists('http_client_service_id', $client) && \is_string($client['http_client_service_id']) && $client['http_client_service_id'] !== '') {
                $client['http_client'] = new Reference($client['http_client_service_id']);
                unset($client['http_client_service_id']);
            }

            if (\array_key_exists('logger_service_id', $client) && \is_string($client['logger_service_id']) && $client['logger_service_id'] !== '') {
                $client['logger'] = new Reference($client['logger_service_id']);
                unset($client['logger_service_id']);
            }

            $entry['client'] = $client;
        }

        return $entry;
    }

    /**
     * @param array<string, mixed>&array{type: string} $entry
     *
     * @return array<string, mixed>&array{type: string}
     */
    private function resolveAzureBlobReferences(array $entry) : array
    {
        if (\array_key_exists('client_service_id', $entry) && \is_string($entry['client_service_id']) && $entry['client_service_id'] !== '') {
            $entry['client'] = new Reference($entry['client_service_id']);
            unset($entry['client_service_id']);
        }

        if (\array_key_exists('client', $entry) && \is_array($entry['client'])) {
            $client = $entry['client'];

            $serviceKeyMap = [
                'http_client_service' => 'http_client',
                'request_factory_service' => 'request_factory',
                'stream_factory_service' => 'stream_factory',
                'logger_service_id' => 'logger',
            ];

            foreach ($serviceKeyMap as $configKey => $resolvedKey) {
                if (\array_key_exists($configKey, $client) && \is_string($client[$configKey]) && $client[$configKey] !== '') {
                    $client[$resolvedKey] = new Reference($client[$configKey]);
                    unset($client[$configKey]);
                }
            }

            $entry['client'] = $client;
        }

        return $entry;
    }

    /**
     * @param array<string, mixed>&array{type: string} $entry
     *
     * @return array<string, mixed>&array{type: string}
     */
    private function resolveServiceReferences(array $entry) : array
    {
        return match ($entry['type']) {
            'aws_s3' => $this->resolveAwsS3References($entry),
            'azure_blob' => $this->resolveAzureBlobReferences($entry),
            default => $entry,
        };
    }
}
