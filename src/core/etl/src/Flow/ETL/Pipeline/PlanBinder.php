<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;

final readonly class PlanBinder
{
    /**
     * @throws SchemaNotDerivableException
     */
    public function bind(Extractor $extractor, Segments $segments): BoundPlan
    {
        $schema = $extractor->schema();
        $bound = new Segments($extractor);

        foreach ($segments->steps() as $step) {
            if ($step instanceof Loader) {
                $bound->add($step);

                continue;
            }

            $boundStep = $step->bind($schema);
            $schema = $boundStep->output;
            $bound->add($boundStep->step);
        }

        return new BoundPlan($bound, $schema);
    }
}
