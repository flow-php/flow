<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\BuildFstabsPass;
use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\RegisterFstabLocatorPass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\DependencyInjection\ServiceLocator;

use function array_keys;

final class RegisterFstabLocatorPassTest extends TestCase
{
    public function test_default_fstab_parameter_is_empty_string_when_null(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter(BuildFstabsPass::CONFIG_PARAMETER, [
            'default_fstab' => null,
            'fstabs' => ['primary' => ['filesystems' => ['memory' => []]]],
        ]);

        (new RegisterFstabLocatorPass())->process($container);

        static::assertSame('', $container->getParameter(RegisterFstabLocatorPass::DEFAULT_FSTAB_PARAMETER));
    }

    public function test_does_nothing_when_config_parameter_missing(): void
    {
        $container = new ContainerBuilder();

        (new RegisterFstabLocatorPass())->process($container);

        static::assertFalse($container->hasDefinition(RegisterFstabLocatorPass::LOCATOR_SERVICE_ID));
        static::assertFalse($container->hasParameter(RegisterFstabLocatorPass::DEFAULT_FSTAB_PARAMETER));
    }

    public function test_exposes_default_fstab_parameter(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter(BuildFstabsPass::CONFIG_PARAMETER, [
            'default_fstab' => 'primary',
            'fstabs' => ['primary' => ['filesystems' => ['memory' => []]]],
        ]);

        (new RegisterFstabLocatorPass())->process($container);

        static::assertSame('primary', $container->getParameter(RegisterFstabLocatorPass::DEFAULT_FSTAB_PARAMETER));
    }

    public function test_registers_locator_with_reference_per_fstab(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter(BuildFstabsPass::CONFIG_PARAMETER, [
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => ['filesystems' => ['memory' => []]],
                'secondary' => ['filesystems' => ['memory' => []]],
            ],
        ]);

        (new RegisterFstabLocatorPass())->process($container);

        $definition = $container->getDefinition(RegisterFstabLocatorPass::LOCATOR_SERVICE_ID);
        static::assertSame(ServiceLocator::class, $definition->getClass());
        static::assertFalse($definition->isPublic());
        static::assertSame(['container.service_locator' => [[]]], $definition->getTags());

        /** @var array<string, Reference> $refs */
        $refs = $definition->getArgument(0);
        static::assertSame(['default', 'secondary'], array_keys($refs));
        static::assertSame('.flow.filesystem.fstab.default', (string) $refs['default']);
        static::assertSame('.flow.filesystem.fstab.secondary', (string) $refs['secondary']);
    }
}
