<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Telemetry\Provider\Composite\CompositeExporter;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

use function is_array;
use function is_string;
use function sprintf;

final class ProfilerSignalCapturePass implements CompilerPassInterface
{
    private const string CAPTURED_EXPORTERS_PARAMETER = 'flow.telemetry.profiler.captured_exporters';

    private const string STORE_SERVICE_ID = 'flow.telemetry.profiler.store';

    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter(self::CAPTURED_EXPORTERS_PARAMETER)) {
            return;
        }

        $capturedIds = $container->getParameter(self::CAPTURED_EXPORTERS_PARAMETER);

        if (!is_array($capturedIds)) {
            return;
        }

        // @mago-expect analysis:mixed-assignment
        foreach ($capturedIds as $exporterId) {
            if (!is_string($exporterId) || $exporterId === '') {
                continue;
            }

            if ($container->hasAlias($exporterId)) {
                throw new RuntimeException(sprintf(
                    'Profiler capture cannot transparently decorate exporter "%s" because it is a service alias '
                    . '(type: service). Decorate the aliased target id instead, or disable profiler capture for it.',
                    $exporterId,
                ));
            }

            if (!$container->hasDefinition($exporterId)) {
                throw new RuntimeException(sprintf(
                    'Profiler capture references exporter service "%s" which is not defined.',
                    $exporterId,
                ));
            }

            $innerId = $exporterId . '.profiler_tee.inner';

            $decorator = new Definition(CompositeExporter::class);
            $decorator->setDecoratedService($exporterId, $innerId);
            $decorator->setArguments([
                [
                    new Reference($innerId),
                    new Reference(self::STORE_SERVICE_ID),
                ],
            ]);

            $container->setDefinition($exporterId . '.profiler_tee', $decorator);
        }
    }
}
