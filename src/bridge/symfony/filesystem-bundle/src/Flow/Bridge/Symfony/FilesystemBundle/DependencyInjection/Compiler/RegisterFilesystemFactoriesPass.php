<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Reference};

final class RegisterFilesystemFactoriesPass implements CompilerPassInterface
{
    public const string REGISTRY_SERVICE_ID = '.flow_filesystem.factory_registry';

    public const string TAG = 'flow_filesystem.factory';

    public function process(ContainerBuilder $container) : void
    {
        if (!$container->hasDefinition(self::REGISTRY_SERVICE_ID)) {
            return;
        }

        $references = [];
        $seen = [];

        foreach ($container->findTaggedServiceIds(self::TAG) as $serviceId => $tags) {
            foreach ($tags as $tag) {
                if (!\array_key_exists('protocol', $tag) || !\is_string($tag['protocol']) || $tag['protocol'] === '') {
                    throw new LogicException(\sprintf(
                        'Service "%s" is tagged with "%s" but is missing a non-empty "protocol" attribute.',
                        $serviceId,
                        self::TAG,
                    ));
                }

                $protocol = $tag['protocol'];

                if (\array_key_exists($protocol, $seen)) {
                    throw new LogicException(\sprintf(
                        'Duplicate filesystem factory for protocol "%s" (services "%s" and "%s").',
                        $protocol,
                        $seen[$protocol],
                        $serviceId,
                    ));
                }

                $seen[$protocol] = $serviceId;
                $references[] = new Reference($serviceId);
            }
        }

        $container->getDefinition(self::REGISTRY_SERVICE_ID)->setArgument(0, $references);
    }
}
