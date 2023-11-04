<?php declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\Optimizer\Optimization;
use Flow\ETL\Transformer;

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

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline
    {
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
