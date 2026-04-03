<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle;

use Flow\Bridge\Symfony\PostgreSqlBundle\Attribute\AsCatalogProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\Compiler\CatalogProviderPass;
use Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\FlowPostgreSqlExtension;
use Symfony\Component\DependencyInjection\{ChildDefinition, ContainerBuilder};
use Symfony\Component\DependencyInjection\Extension\ExtensionInterface;
use Symfony\Component\HttpKernel\Bundle\Bundle;

final class FlowPostgreSqlBundle extends Bundle
{
    #[\Override]
    public function build(ContainerBuilder $container) : void
    {
        parent::build($container);

        $container->addCompilerPass(new CatalogProviderPass());

        $container->registerAttributeForAutoconfiguration(
            AsCatalogProvider::class,
            static function (ChildDefinition $definition, AsCatalogProvider $attribute, \Reflector $reflector) : void {
                $definition->addTag('flow.postgresql.catalog_provider');
            },
        );
    }

    #[\Override]
    public function getContainerExtension() : ExtensionInterface
    {
        return new FlowPostgreSqlExtension();
    }
}
