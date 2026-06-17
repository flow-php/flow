<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

use function array_key_exists;
use function array_keys;
use function implode;
use function is_array;
use function is_string;
use function sprintf;

final class ResolveFilesystemArgumentsPass implements CompilerPassInterface
{
    public const string TAG = 'flow.filesystem.as_filesystem';

    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter(BuildFstabsPass::CONFIG_PARAMETER)) {
            return;
        }

        $config = $container->getParameter(BuildFstabsPass::CONFIG_PARAMETER);

        if (!is_array($config)) {
            return;
        }

        $fstabs = is_array($config['fstabs'] ?? null) ? $config['fstabs'] : [];
        $defaultFstab = is_string($config['default_fstab'] ?? null) ? $config['default_fstab'] : null;

        foreach ($container->findTaggedServiceIds(self::TAG) as $serviceId => $tags) {
            $definition = $container->getDefinition($serviceId);

            // @mago-expect analysis:mixed-assignment
            foreach ($tags as $tag) {
                if (!is_array($tag)) {
                    continue;
                }

                $argument = is_string($tag['argument'] ?? null) ? $tag['argument'] : '';
                $mount = is_string($tag['mount'] ?? null) ? $tag['mount'] : '';
                $fstab = is_string($tag['fstab'] ?? null) && $tag['fstab'] !== '' ? $tag['fstab'] : $defaultFstab;

                if ($fstab === null) {
                    throw new LogicException(sprintf(
                        'Service "%s" uses #[AsFilesystem(\'%s\')] without an fstab, but no default fstab is configured.',
                        $serviceId,
                        $mount,
                    ));
                }

                if (!array_key_exists($fstab, $fstabs) || !is_array($fstabs[$fstab])) {
                    throw new LogicException(sprintf(
                        'Service "%s": #[AsFilesystem] references fstab "%s" which is not configured. Available fstabs: [%s].',
                        $serviceId,
                        $fstab,
                        implode(', ', array_keys($fstabs)),
                    ));
                }

                $mounts = is_array($fstabs[$fstab]['filesystems'] ?? null) ? $fstabs[$fstab]['filesystems'] : [];

                if (!array_key_exists($mount, $mounts)) {
                    throw new LogicException(sprintf(
                        'Service "%s": #[AsFilesystem] references mount "%s" which is not mounted in fstab "%s". Available mounts: [%s].',
                        $serviceId,
                        $mount,
                        $fstab,
                        implode(', ', array_keys($mounts)),
                    ));
                }

                $definition->setArgument(
                    '$' . $argument,
                    new Reference(BuildFstabsPass::FS_SERVICE_PREFIX . $fstab . '.' . $mount),
                );
            }
        }
    }
}
