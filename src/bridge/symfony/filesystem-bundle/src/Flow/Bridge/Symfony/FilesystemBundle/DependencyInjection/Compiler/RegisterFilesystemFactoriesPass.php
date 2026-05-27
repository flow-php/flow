<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

use function array_key_exists;
use function is_array;
use function is_string;
use function sprintf;

final class RegisterFilesystemFactoriesPass implements CompilerPassInterface
{
    public const string REGISTRY_SERVICE_ID = '.flow.filesystem.factory_registry';

    public const string TAG = 'flow.filesystem.factory';

    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasDefinition(self::REGISTRY_SERVICE_ID)) {
            return;
        }

        $references = [];
        $seen = [];

        foreach ($container->findTaggedServiceIds(self::TAG) as $serviceId => $tags) {
            // @mago-expect analysis:mixed-assignment
            foreach ($tags as $tag) {
                if (!is_array($tag) || !array_key_exists('type', $tag) || !is_string($tag['type']) || $tag['type'] === '') {
                    throw new LogicException(sprintf(
                        'Service "%s" is tagged with "%s" but is missing a non-empty "type" attribute.',
                        $serviceId,
                        self::TAG,
                    ));
                }

                $type = $tag['type'];

                if (array_key_exists($type, $seen)) {
                    throw new LogicException(sprintf(
                        'Duplicate filesystem factory for type "%s" (services "%s" and "%s").',
                        $type,
                        $seen[$type],
                        $serviceId,
                    ));
                }

                $seen[$type] = $serviceId;
                $references[] = new Reference($serviceId);
            }
        }

        $container->getDefinition(self::REGISTRY_SERVICE_ID)->setArgument(0, $references);
    }
}
