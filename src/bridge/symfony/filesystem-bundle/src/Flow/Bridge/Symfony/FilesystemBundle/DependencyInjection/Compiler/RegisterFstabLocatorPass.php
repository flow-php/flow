<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference, ServiceLocator};

final class RegisterFstabLocatorPass implements CompilerPassInterface
{
    public const string DEFAULT_FSTAB_PARAMETER = 'flow_filesystem.default_fstab';

    public const string LOCATOR_SERVICE_ID = 'flow_filesystem.fstab_locator';

    public function process(ContainerBuilder $container) : void
    {
        if (!$container->hasParameter(BuildFstabsPass::CONFIG_PARAMETER)) {
            return;
        }

        /** @var array{default_fstab: null|string, fstabs: array<string, array<string, mixed>>} $config */
        $config = $container->getParameter(BuildFstabsPass::CONFIG_PARAMETER);

        $references = [];

        foreach (\array_keys($config['fstabs']) as $fstabName) {
            $references[$fstabName] = new Reference(BuildFstabsPass::FSTAB_SERVICE_PREFIX . $fstabName);
        }

        $locator = new Definition(ServiceLocator::class);
        $locator->setArguments([$references]);
        $locator->addTag('container.service_locator');
        $locator->setPublic(false);

        $container->setDefinition(self::LOCATOR_SERVICE_ID, $locator);

        $container->setParameter(self::DEFAULT_FSTAB_PARAMETER, (string) ($config['default_fstab'] ?? ''));
    }
}
