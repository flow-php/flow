<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FstabBuilder;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Telemetry\FilesystemTelemetryConfig;
use Flow\Filesystem\Telemetry\FilesystemTelemetryOptions;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

use function array_key_exists;
use function array_keys;
use function implode;
use function is_array;
use function is_string;
use function lcfirst;
use function sprintf;
use function str_replace;
use function ucwords;

final class BuildFstabsPass implements CompilerPassInterface
{
    public const string CONFIG_PARAMETER = 'flow.filesystem.config';

    public const string FSTAB_SERVICE_PREFIX = '.flow.filesystem.fstab.';

    public const string TELEMETRY_CONFIG_SERVICE_PREFIX = '.flow.filesystem.telemetry_config.';

    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter(self::CONFIG_PARAMETER)) {
            return;
        }

        $config = $container->getParameter(self::CONFIG_PARAMETER);

        if (!is_array($config)) {
            return;
        }

        $fstabs = is_array($config['fstabs'] ?? null) ? $config['fstabs'] : [];
        $defaultFstab = is_string($config['default_fstab'] ?? null) ? $config['default_fstab'] : null;

        if ($defaultFstab !== null && !array_key_exists($defaultFstab, $fstabs)) {
            throw new LogicException(sprintf(
                'flow_filesystem: default_fstab "%s" does not match any configured fstab. Available: [%s].',
                $defaultFstab,
                implode(', ', array_keys($fstabs)),
            ));
        }

        $availableTypes = $this->collectAvailableTypes($container);

        // @mago-expect analysis:mixed-assignment
        foreach ($fstabs as $fstabName => $fstabConfig) {
            $fstabNameStr = (string) $fstabName;
            $resolvedFilesystems = [];

            if (!is_array($fstabConfig)) {
                continue;
            }

            $filesystems = is_array($fstabConfig['filesystems'] ?? null) ? $fstabConfig['filesystems'] : [];

            // @mago-expect analysis:mixed-assignment
            foreach ($filesystems as $mountName => $entry) {
                if (!is_array($entry)) {
                    continue;
                }

                $entryType = is_string($entry['type'] ?? null) ? $entry['type'] : '';

                if (!array_key_exists($entryType, $availableTypes)) {
                    throw new LogicException(sprintf(
                        'Fstab "%s" mount "%s": no filesystem factory registered for type "%s". Available types: [%s].',
                        $fstabNameStr,
                        (string) $mountName,
                        $entryType,
                        implode(', ', array_keys($availableTypes)),
                    ));
                }

                $entry['type'] = $entryType;
                $resolvedFilesystems[(string) $mountName] = $this->resolveServiceReferences($entry);
            }

            // @mago-expect analysis:mixed-assignment
            $telemetryRaw = $fstabConfig['telemetry'] ?? [];
            $telemetryArray = is_array($telemetryRaw) ? $telemetryRaw : [];

            $telemetryReference = $this->buildTelemetryConfigReference($container, $fstabNameStr, $telemetryArray);

            $definition = new Definition(FilesystemTable::class);
            $definition->setFactory([FstabBuilder::class, 'build']);
            $definition->setArguments([
                new Reference(RegisterFilesystemFactoriesPass::REGISTRY_SERVICE_ID),
                $fstabNameStr,
                $resolvedFilesystems,
                $telemetryReference,
            ]);
            $definition->setPublic(false);

            $serviceId = self::FSTAB_SERVICE_PREFIX . $fstabNameStr;
            $container->setDefinition($serviceId, $definition);

            $aliasId = FilesystemTable::class . ' $' . $this->camelCase($fstabNameStr) . 'Fstab';
            $container->setAlias($aliasId, $serviceId)->setPublic(true);
        }

        if ($defaultFstab !== null) {
            $container->setAlias(FilesystemTable::class, self::FSTAB_SERVICE_PREFIX . $defaultFstab)->setPublic(true);
        }
    }

    /**
     * @param array<array-key, mixed> $telemetry
     */
    private function buildTelemetryConfigReference(
        ContainerBuilder $container,
        string $fstabName,
        array $telemetry,
    ): ?Reference {
        if (($telemetry['enabled'] ?? false) !== true) {
            return null;
        }

        // @mago-expect analysis:mixed-assignment
        $telemetryServiceId = $telemetry['telemetry_service_id'] ?? null;
        // @mago-expect analysis:mixed-assignment
        $clockServiceId = $telemetry['clock_service_id'] ?? null;

        if (!is_string($telemetryServiceId) || $telemetryServiceId === '') {
            throw new LogicException(sprintf(
                'Fstab "%s" telemetry: telemetry_service_id must be a non-empty string when enabled.',
                $fstabName,
            ));
        }

        if (!is_string($clockServiceId) || $clockServiceId === '') {
            throw new LogicException(sprintf(
                'Fstab "%s" telemetry: clock_service_id must be a non-empty string when enabled.',
                $fstabName,
            ));
        }

        // @mago-expect analysis:mixed-assignment
        $optionsRaw = $telemetry['options'] ?? [];
        $optionsConfig = is_array($optionsRaw) ? $optionsRaw : [];
        $traceStreams = ($optionsConfig['trace_streams'] ?? true) === true;
        $collectMetrics = ($optionsConfig['collect_metrics'] ?? true) === true;

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

    private function camelCase(string $name): string
    {
        return lcfirst(str_replace(' ', '', ucwords(str_replace('_', ' ', $name))));
    }

    /**
     * @return array<string, string>
     */
    private function collectAvailableTypes(ContainerBuilder $container): array
    {
        $types = [];

        foreach ($container->findTaggedServiceIds(RegisterFilesystemFactoriesPass::TAG) as $serviceId => $tags) {
            // @mago-expect analysis:mixed-assignment
            foreach ($tags as $tag) {
                if (
                    is_array($tag)
                    && array_key_exists('type', $tag)
                    && is_string($tag['type'])
                    && $tag['type'] !== ''
                ) {
                    $types[$tag['type']] = $serviceId;
                }
            }
        }

        return $types;
    }

    /**
     * @param array<array-key, mixed> $entry
     *
     * @return array<array-key, mixed>
     */
    private function resolveAwsS3References(array $entry): array
    {
        if (
            array_key_exists('client_service_id', $entry)
            && is_string($entry['client_service_id'])
            && $entry['client_service_id'] !== ''
        ) {
            $entry['client'] = new Reference($entry['client_service_id']);
            unset($entry['client_service_id']);
        }

        if (array_key_exists('client', $entry) && is_array($entry['client'])) {
            $client = $entry['client'];

            if (
                array_key_exists('http_client_service_id', $client)
                && is_string($client['http_client_service_id'])
                && $client['http_client_service_id'] !== ''
            ) {
                $client['http_client'] = new Reference($client['http_client_service_id']);
                unset($client['http_client_service_id']);
            }

            if (
                array_key_exists('logger_service_id', $client)
                && is_string($client['logger_service_id'])
                && $client['logger_service_id'] !== ''
            ) {
                $client['logger'] = new Reference($client['logger_service_id']);
                unset($client['logger_service_id']);
            }

            $entry['client'] = $client;
        }

        return $entry;
    }

    /**
     * @param array<array-key, mixed> $entry
     *
     * @return array<array-key, mixed>
     */
    private function resolveAzureBlobReferences(array $entry): array
    {
        if (
            array_key_exists('client_service_id', $entry)
            && is_string($entry['client_service_id'])
            && $entry['client_service_id'] !== ''
        ) {
            $entry['client'] = new Reference($entry['client_service_id']);
            unset($entry['client_service_id']);
        }

        if (array_key_exists('client', $entry) && is_array($entry['client'])) {
            $client = $entry['client'];

            $serviceKeyMap = [
                'http_client_service' => 'http_client',
                'request_factory_service' => 'request_factory',
                'stream_factory_service' => 'stream_factory',
                'logger_service_id' => 'logger',
            ];

            foreach ($serviceKeyMap as $configKey => $resolvedKey) {
                if (
                    array_key_exists($configKey, $client)
                    && is_string($client[$configKey])
                    && $client[$configKey] !== ''
                ) {
                    $client[$resolvedKey] = new Reference($client[$configKey]);
                    unset($client[$configKey]);
                }
            }

            $entry['client'] = $client;
        }

        return $entry;
    }

    /**
     * @param array<array-key, mixed> $entry
     *
     * @return array<array-key, mixed>
     */
    private function resolveServiceReferences(array $entry): array
    {
        return match ($entry['type']) {
            'aws_s3' => $this->resolveAwsS3References($entry),
            'azure_blob' => $this->resolveAzureBlobReferences($entry),
            default => $entry,
        };
    }
}
