<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures;

use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function array_filter;
use function array_keys;
use function array_merge;
use function array_values;
use function str_ends_with;
use function str_starts_with;

final class CompiledIdCollectorPass implements CompilerPassInterface
{
    /** @var array<string> */
    public array $ids = [];

    /** @var array<string> */
    public array $parameters = [];

    public function process(ContainerBuilder $container): void
    {
        $this->ids = array_merge(array_keys($container->getDefinitions()), array_keys($container->getAliases()));

        /** @var array<string> $parameterNames */
        $parameterNames = array_keys($container->getParameterBag()->all());
        $this->parameters = $parameterNames;
    }

    /**
     * @return array<string>
     */
    public function flowTelemetryIds(): array
    {
        return array_values(array_filter(
            $this->ids,
            static fn(string $id): bool => (
                str_starts_with($id, 'flow.telemetry')
                || str_ends_with($id, '.flow_telemetry')
                || $id === Telemetry::class
            ),
        ));
    }

    /**
     * @return array<string>
     */
    public function flowTelemetryParameters(): array
    {
        return array_values(array_filter($this->parameters, static fn(string $name): bool => str_starts_with(
            $name,
            'flow.telemetry',
        )));
    }
}
