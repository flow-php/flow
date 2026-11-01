<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

use function array_merge;
use function array_unique;

/**
 * Extracted contexts are merged.
 */
final readonly class CompositePropagator implements Propagator
{
    /**
     * @param array<Propagator> $propagators
     */
    public function __construct(
        private array $propagators,
    ) {}

    /**
     * @param Carrier<mixed> $carrier
     */
    public function extract(Carrier $carrier): PropagationContext
    {
        $result = new PropagationContext();

        foreach ($this->propagators as $propagator) {
            $result = $result->merge($propagator->extract($carrier));
        }

        return $result;
    }

    /**
     * @return array<string>
     */
    public function fields(): array
    {
        $fields = [];

        foreach ($this->propagators as $propagator) {
            $fields = array_merge($fields, $propagator->fields());
        }

        return array_unique($fields);
    }

    /**
     * @param Carrier<mixed> $carrier
     */
    public function inject(PropagationContext $context, Carrier $carrier): void
    {
        foreach ($this->propagators as $propagator) {
            $propagator->inject($context, $carrier);
        }
    }
}
