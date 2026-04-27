<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\{BuildFstabsPass, RegisterFilesystemFactoriesPass, RegisterFstabLocatorPass};
use Flow\Bridge\Symfony\FilesystemCache\FlowFilesystemCacheAdapter;
use Flow\Filesystem\{Filesystem, Path};
use Symfony\Component\Config\Definition\Builder\{NodeDefinition, TreeBuilder};
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\DependencyInjection\{ChildDefinition, ContainerBuilder, Definition, Reference};
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;

final class FlowFilesystemBundle extends AbstractBundle
{
    private const string MOUNT_REGEX = '/^[a-zA-Z][a-zA-Z0-9+.-]+$/';

    #[\Override]
    public function build(ContainerBuilder $container) : void
    {
        parent::build($container);

        $container->addCompilerPass(new RegisterFilesystemFactoriesPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, 10);
        $container->addCompilerPass(new BuildFstabsPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, 0);
        $container->addCompilerPass(new RegisterFstabLocatorPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, -10);

        $container->registerAttributeForAutoconfiguration(
            AsFilesystemFactory::class,
            static function (ChildDefinition $definition, AsFilesystemFactory $attribute, \Reflector $reflector) : void {
                $definition->addTag(RegisterFilesystemFactoriesPass::TAG, ['type' => $attribute->type]);
            },
        );
    }

    #[\Override]
    public function configure(DefinitionConfigurator $definition) : void
    {
        $definition->rootNode()
            ->children()
                ->scalarNode('default_fstab')->defaultNull()->end()
                ->arrayNode('fstabs')
                    ->isRequired()
                    ->requiresAtLeastOneElement()
                    ->useAttributeAsKey('name')
                    ->validate()
                        ->ifTrue(static function (array $fstabs) : bool {
                            foreach (\array_keys($fstabs) as $name) {
                                if (!\is_string($name) || $name === '') {
                                    return true;
                                }
                            }

                            return false;
                        })
                        ->thenInvalid('Fstab name must be a non-empty string.')
                    ->end()
                    ->arrayPrototype()
                        ->children()
                            ->arrayNode('filesystems')
                                ->isRequired()
                                ->requiresAtLeastOneElement()
                                ->normalizeKeys(false)
                                ->useAttributeAsKey('mount')
                                ->validate()
                                    ->ifTrue(static function (array $filesystems) : bool {
                                        foreach (\array_keys($filesystems) as $mount) {
                                            if (!\is_string($mount) || \preg_match(self::MOUNT_REGEX, $mount) !== 1) {
                                                return true;
                                            }
                                        }

                                        return false;
                                    })
                                    ->thenInvalid('Mount name must match ' . self::MOUNT_REGEX . '.')
                                ->end()
                                ->arrayPrototype()
                                    ->ignoreExtraKeys(false)
                                    ->children()
                                        ->scalarNode('type')
                                            ->isRequired()
                                            ->cannotBeEmpty()
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                            ->append($this->telemetryNode())
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('cache')
                    ->info('Defines filesystem-backed Symfony Cache pools. Requires flow-php/symfony-filesystem-cache-bridge.')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->arrayNode('pools')
                            ->info('Named cache pools. Each becomes a service "flow.filesystem.cache.pool.<name>" usable as adapter: <id> in framework.cache.pools.')
                            ->useAttributeAsKey('name')
                            ->arrayPrototype()
                                ->children()
                                    ->scalarNode('fstab')
                                        ->defaultNull()
                                        ->info('Fstab name. Defaults to the bundle\'s resolved default fstab when null.')
                                    ->end()
                                    ->scalarNode('filesystem')
                                        ->isRequired()
                                        ->cannotBeEmpty()
                                        ->info('Mount protocol of the filesystem within the chosen fstab (the YAML key under `filesystems:`).')
                                    ->end()
                                    ->scalarNode('path')
                                        ->isRequired()
                                        ->cannotBeEmpty()
                                        ->info('Base directory inside the chosen filesystem where cache files are stored.')
                                    ->end()
                                    ->scalarNode('namespace')
                                        ->defaultValue('')
                                        ->info('Cache pool namespace. Allowed chars: -+.A-Za-z0-9')
                                    ->end()
                                    ->integerNode('default_lifetime')->defaultValue(0)->min(0)->end()
                                    ->scalarNode('marshaller_service_id')->defaultNull()->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();
    }

    /**
     * @param array<string, mixed> $config
     */
    #[\Override]
    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder) : void
    {
        /** @var array<string, array{filesystems: array<string, array<string, mixed>>}> $fstabs */
        $fstabs = $config['fstabs'] ?? [];
        /** @var null|string $defaultFstab */
        $defaultFstab = $config['default_fstab'] ?? null;

        $config['fstabs'] = $fstabs;

        if ($defaultFstab === null) {
            if (\array_key_exists('default', $fstabs)) {
                $defaultFstab = 'default';
            } elseif (\count($fstabs) === 1) {
                $defaultFstab = (string) \array_key_first($fstabs);
            } else {
                throw new InvalidConfigurationException(\sprintf(
                    'flow_filesystem: no `default_fstab` was set and no fstab named "default" exists. Available fstabs: [%s].',
                    \implode(', ', \array_keys($fstabs))
                ));
            }
        } elseif (!\array_key_exists($defaultFstab, $fstabs)) {
            throw new InvalidConfigurationException(\sprintf(
                'flow_filesystem: `default_fstab` is set to "%s" but no such fstab exists. Available fstabs: [%s].',
                $defaultFstab,
                \implode(', ', \array_keys($fstabs))
            ));
        }

        $config['default_fstab'] = $defaultFstab;

        $builder->setParameter('flow.filesystem.config', $config);

        $container->import(__DIR__ . '/Resources/config/services.php');

        /** @var array{pools?: array<string, array{fstab: ?string, filesystem: string, path: string, namespace: string, default_lifetime: int, marshaller_service_id: ?string}>} $cacheConfig */
        $cacheConfig = $config['cache'] ?? [];
        $pools = $cacheConfig['pools'] ?? [];

        if ($pools !== []) {
            $this->registerCachePools($pools, $fstabs, $defaultFstab, $builder);
        }
    }

    /**
     * @param array<string, array{fstab: ?string, filesystem: string, path: string, namespace: string, default_lifetime: int, marshaller_service_id: ?string}> $pools
     * @param array<string, array{filesystems: array<string, array<string, mixed>>}> $fstabs
     */
    private function registerCachePools(array $pools, array $fstabs, string $defaultFstab, ContainerBuilder $builder) : void
    {
        if (!\class_exists(FlowFilesystemCacheAdapter::class)) {
            throw new InvalidConfigurationException('flow_filesystem.cache.pools is configured but flow-php/symfony-filesystem-cache-bridge is not installed. Run composer require flow-php/symfony-filesystem-cache-bridge.');
        }

        foreach ($pools as $name => $poolConfig) {
            $fstabName = $poolConfig['fstab'] ?? $defaultFstab;

            if (!\array_key_exists($fstabName, $fstabs)) {
                throw new InvalidConfigurationException(\sprintf(
                    'flow_filesystem.cache.pools.%s: fstab "%s" is not declared. Available fstabs: [%s].',
                    $name,
                    $fstabName,
                    \implode(', ', \array_keys($fstabs)),
                ));
            }

            $mounts = $fstabs[$fstabName]['filesystems'];

            if (!\array_key_exists($poolConfig['filesystem'], $mounts)) {
                throw new InvalidConfigurationException(\sprintf(
                    'flow_filesystem.cache.pools.%s: filesystem "%s" is not mounted in fstab "%s". Available mounts: [%s].',
                    $name,
                    $poolConfig['filesystem'],
                    $fstabName,
                    \implode(', ', \array_keys($mounts)),
                ));
            }

            $filesystemDef = new Definition(Filesystem::class);
            $filesystemDef->setFactory([new Reference(BuildFstabsPass::FSTAB_SERVICE_PREFIX . $fstabName), 'for']);
            $filesystemDef->setArguments([$poolConfig['filesystem']]);
            $builder->setDefinition("flow.filesystem.cache.pool.{$name}.filesystem", $filesystemDef);

            $pathDef = new Definition(Path::class);
            $pathDef->setFactory([Path::class, 'from']);
            $pathDef->setArguments([$poolConfig['path']]);
            $builder->setDefinition("flow.filesystem.cache.pool.{$name}.path", $pathDef);

            $adapterDef = new Definition(FlowFilesystemCacheAdapter::class, [
                new Reference("flow.filesystem.cache.pool.{$name}.filesystem"),
                new Reference("flow.filesystem.cache.pool.{$name}.path"),
                $poolConfig['namespace'],
                $poolConfig['default_lifetime'],
                $poolConfig['marshaller_service_id'] !== null
                    ? new Reference($poolConfig['marshaller_service_id'])
                    : null,
            ]);
            $adapterDef->setPublic(true);
            $builder->setDefinition("flow.filesystem.cache.pool.{$name}", $adapterDef);
        }
    }

    private function telemetryNode() : NodeDefinition
    {
        $builder = new TreeBuilder('telemetry');

        $builder->getRootNode()
            ->addDefaultsIfNotSet()
            ->children()
                ->booleanNode('enabled')->defaultFalse()->end()
                ->scalarNode('telemetry_service_id')->defaultNull()->end()
                ->scalarNode('clock_service_id')->defaultNull()->end()
                ->arrayNode('options')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->booleanNode('trace_streams')->defaultTrue()->end()
                        ->booleanNode('collect_metrics')->defaultTrue()->end()
                    ->end()
                ->end()
            ->end()
            ->validate()
                ->ifTrue(static fn (array $v) : bool => ($v['enabled'] ?? false) === true && (!\is_string($v['telemetry_service_id'] ?? null) || $v['telemetry_service_id'] === ''))
                ->thenInvalid('telemetry.enabled=true requires a non-empty `telemetry_service_id`.')
            ->end()
            ->validate()
                ->ifTrue(static fn (array $v) : bool => ($v['enabled'] ?? false) === true && (!\is_string($v['clock_service_id'] ?? null) || $v['clock_service_id'] === ''))
                ->thenInvalid('telemetry.enabled=true requires a non-empty `clock_service_id`.')
            ->end();

        return $builder->getRootNode();
    }
}
