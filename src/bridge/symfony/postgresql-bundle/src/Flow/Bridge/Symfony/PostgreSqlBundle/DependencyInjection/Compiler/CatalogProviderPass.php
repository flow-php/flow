<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\Compiler;

use Flow\PostgreSql\Schema\ChainCatalogProvider;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference};

final class CatalogProviderPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container) : void
    {
        $providerRefs = [];

        foreach ($container->findTaggedServiceIds('flow.postgresql.catalog_provider') as $serviceId => $tags) {
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
            throw new \LogicException('No catalog providers found. Register at least one catalog provider using #[AsCatalogProvider] attribute, "flow.postgresql.catalog_provider" tag, or "catalog_providers" configuration.');
        }

        $chainDef = new Definition(ChainCatalogProvider::class, $providerRefs);
        $chainDef->setPublic(true);
        $container->setDefinition('flow.postgresql.migrations.catalog_provider', $chainDef);
    }
}
