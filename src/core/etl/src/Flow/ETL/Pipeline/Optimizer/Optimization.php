<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\{Loader, Pipeline, Transformer};

interface Optimization
{
    public function isFor(Loader|Transformer $element, Pipeline $pipeline) : bool;

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline;
}
