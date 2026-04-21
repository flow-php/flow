<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\{BuildFstabsPass, RegisterFilesystemFactoriesPass, RegisterFstabLocatorPass};
use Symfony\Component\Config\Definition\Builder\{NodeDefinition, TreeBuilder};
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\DependencyInjection\{ChildDefinition, ContainerBuilder};
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

        $builder->setParameter('flow_filesystem.config', $config);

        $container->import(__DIR__ . '/Resources/config/services.php');
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
