<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\FrameworkLoggerPass;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\HttpKernel\Log\Logger as SymfonyDefaultLogger;

#[CoversClass(FrameworkLoggerPass::class)]
final class FrameworkLoggerPassTest extends TestCase
{
    public function test_leaves_symfony_logger_untouched_when_telemetry_is_disabled(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.enabled', false);
        $container->setDefinition('logger', new Definition(SymfonyDefaultLogger::class));

        (new FrameworkLoggerPass())->process($container);

        static::assertFalse($container->hasAlias('logger'));
        static::assertSame(SymfonyDefaultLogger::class, $container->getDefinition('logger')->getClass());
    }

    public function test_aliases_symfony_logger_when_telemetry_is_enabled(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.enabled', true);
        $container->setDefinition('logger', new Definition(SymfonyDefaultLogger::class));

        (new FrameworkLoggerPass())->process($container);

        static::assertTrue($container->hasAlias('logger'));
        static::assertSame('flow.telemetry.default.logger.psr3', (string) $container->getAlias('logger'));
    }

    public function test_aliases_symfony_logger_when_enabled_parameter_is_absent(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('logger', new Definition(SymfonyDefaultLogger::class));

        (new FrameworkLoggerPass())->process($container);

        static::assertTrue($container->hasAlias('logger'));
    }
}
