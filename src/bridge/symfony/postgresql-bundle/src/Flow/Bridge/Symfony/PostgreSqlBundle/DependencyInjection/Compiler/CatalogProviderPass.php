<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\Compiler;

use Flow\PostgreSql\Client\Context;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\ChainCatalogProvider;
use LogicException;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

use function count;

final class CatalogProviderPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        $providerRefs = [];

        foreach ($container->findTaggedServiceIds('flow.postgresql.catalog_provider') as $serviceId => $_tags) {
            $providerRefs[] = new Reference($serviceId);
        }

        if ($container->hasParameter('flow.postgresql.catalog_provider.service_ids')) {
            /** @var list<string> $serviceIds */
            $serviceIds = $container->getParameter('flow.postgresql.catalog_provider.service_ids');

            foreach ($serviceIds as $serviceId) {
                $providerRefs[] = new Reference($serviceId);
            }
        }

        if ($providerRefs === [] && $container->hasParameter('flow.postgresql.migrations.connections')) {
            throw new LogicException(
                'No catalog providers found. Register at least one catalog provider using #[AsCatalogProvider] attribute, "flow.postgresql.catalog_provider" tag, or "catalog_providers" configuration.',
            );
        }

        if ($providerRefs === []) {
            return;
        }

        $chainDef = new Definition(ChainCatalogProvider::class, $providerRefs);
        $chainDef->setPublic(true);
        $container->setDefinition('flow.postgresql.catalog_provider', $chainDef);

        $catalogDef = new Definition(Catalog::class);
        $catalogDef->setFactory([new Reference('flow.postgresql.catalog_provider'), 'get']);
        $catalogDef->setPublic(true);
        $container->setDefinition('flow.postgresql.catalog', $catalogDef);

        if ($container->hasParameter('flow.postgresql.connections')) {
            /** @var list<string> $connections */
            $connections = $container->getParameter('flow.postgresql.connections');

            foreach ($connections as $name) {
                $this->attachCatalogToConnectionContext($container, $name);
            }
        }
    }

    private function attachCatalogToConnectionContext(ContainerBuilder $container, string $name): void
    {
        $contextId = "flow.postgresql.{$name}.context";
        $clientId = "flow.postgresql.{$name}.client";
        $catalogRef = new Reference('flow.postgresql.catalog');

        if ($container->hasDefinition($contextId)) {
            $container->getDefinition($contextId)->replaceArgument(0, $catalogRef);

            return;
        }

        $container->setDefinition($contextId, new Definition(Context::class, [$catalogRef, []]));

        if (!$container->hasDefinition($clientId)) {
            return;
        }

        $clientDef = $container->getDefinition($clientId);
        $arguments = $clientDef->getArguments();

        while (count($arguments) < 2) {
            $arguments[] = null;
        }

        $arguments[2] = new Reference($contextId);
        $clientDef->setArguments($arguments);
    }
}
