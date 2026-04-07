<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\{BuildFstabsPass, RegisterFstabLocatorPass};
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Reference, ServiceLocator};

final class RegisterFstabLocatorPassTest extends TestCase
{
    public function test_default_fstab_parameter_is_empty_string_when_null() : void
    {
        $container = new ContainerBuilder();
        $container->setParameter(BuildFstabsPass::CONFIG_PARAMETER, [
            'default_fstab' => null,
            'fstabs' => ['primary' => ['filesystems' => ['memory' => []]]],
        ]);

        (new RegisterFstabLocatorPass())->process($container);

        self::assertSame('', $container->getParameter(RegisterFstabLocatorPass::DEFAULT_FSTAB_PARAMETER));
    }

    public function test_does_nothing_when_config_parameter_missing() : void
    {
        $container = new ContainerBuilder();

        (new RegisterFstabLocatorPass())->process($container);

        self::assertFalse($container->hasDefinition(RegisterFstabLocatorPass::LOCATOR_SERVICE_ID));
        self::assertFalse($container->hasParameter(RegisterFstabLocatorPass::DEFAULT_FSTAB_PARAMETER));
    }

    public function test_exposes_default_fstab_parameter() : void
    {
        $container = new ContainerBuilder();
        $container->setParameter(BuildFstabsPass::CONFIG_PARAMETER, [
            'default_fstab' => 'primary',
            'fstabs' => ['primary' => ['filesystems' => ['memory' => []]]],
        ]);

        (new RegisterFstabLocatorPass())->process($container);

        self::assertSame('primary', $container->getParameter(RegisterFstabLocatorPass::DEFAULT_FSTAB_PARAMETER));
    }

    public function test_registers_locator_with_reference_per_fstab() : void
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
        self::assertSame(ServiceLocator::class, $definition->getClass());
        self::assertFalse($definition->isPublic());
        self::assertSame(['container.service_locator' => [[]]], $definition->getTags());

        /** @var array<string, Reference> $refs */
        $refs = $definition->getArgument(0);
        self::assertSame(['default', 'secondary'], \array_keys($refs));
        self::assertSame('.flow_filesystem.fstab.default', (string) $refs['default']);
        self::assertSame('.flow_filesystem.fstab.secondary', (string) $refs['secondary']);
    }
}
