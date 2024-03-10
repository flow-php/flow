<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Pipeline\Optimizer\Optimization;
use Flow\ETL\{Loader, Pipeline, Transformer};

final class Optimizer
{
    /**
     * @var array<Optimization>
     */
    private array $optimizations;

    public function __construct(Optimization ...$optimizations)
    {
        $this->optimizations = $optimizations;
    }

    public function disabled() : self
    {
        return new self();
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline
    {
        if (!\count($this->optimizations)) {
            return $pipeline->add($element);
        }

        $optimized = false;

        foreach ($this->optimizations as $optimization) {
            if ($optimization->isFor($element, $pipeline)) {
                $pipeline = $optimization->optimize($element, $pipeline);
                $optimized = true;
            }
        }

        return $optimized ? $pipeline : $pipeline->add($element);
    }
}
