<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ProfilerSignalCapturePass;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Telemetry\Provider\Composite\CompositeExporter;
use Flow\Telemetry\Provider\Void\VoidExporter;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

#[CoversClass(ProfilerSignalCapturePass::class)]
final class ProfilerSignalCapturePassTest extends TestCase
{
    public function test_no_op_when_parameter_absent(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('flow.telemetry.exporter.primary', new Definition(VoidExporter::class));

        (new ProfilerSignalCapturePass())->process($container);

        static::assertFalse($container->hasDefinition('flow.telemetry.exporter.primary.profiler_tee'));
    }

    public function test_no_op_when_captured_list_empty(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.profiler.captured_exporters', []);

        (new ProfilerSignalCapturePass())->process($container);

        static::assertFalse($container->hasDefinition('flow.telemetry.exporter.primary.profiler_tee'));
    }

    public function test_decorates_captured_exporter_into_a_composite(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('flow.telemetry.exporter.primary', new Definition(VoidExporter::class));
        $container->setParameter('flow.telemetry.profiler.captured_exporters', ['flow.telemetry.exporter.primary']);

        (new ProfilerSignalCapturePass())->process($container);

        $decorator = $container->getDefinition('flow.telemetry.exporter.primary.profiler_tee');
        static::assertSame(CompositeExporter::class, $decorator->getClass());
        $decorated = $decorator->getDecoratedService();
        static::assertNotNull($decorated);
        static::assertSame('flow.telemetry.exporter.primary', $decorated[0]);
        static::assertSame('flow.telemetry.exporter.primary.profiler_tee.inner', $decorated[1]);
    }

    public function test_throws_when_captured_exporter_is_an_alias(): void
    {
        $container = new ContainerBuilder();
        $container->setAlias('flow.telemetry.exporter.primary', 'some.real.exporter');
        $container->setParameter('flow.telemetry.profiler.captured_exporters', ['flow.telemetry.exporter.primary']);

        $this->expectException(RuntimeException::class);

        (new ProfilerSignalCapturePass())->process($container);
    }

    public function test_throws_when_captured_exporter_is_not_defined(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.profiler.captured_exporters', ['flow.telemetry.exporter.missing']);

        $this->expectException(RuntimeException::class);

        (new ProfilerSignalCapturePass())->process($container);
    }
}
