<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\Plan;
use Flow\ETL\Schema;

/**
 * A source whose rows are another plan's output. The planner runs that plan as a dependency of the
 * pipeline that reads it, instead of calling extract().
 */
interface NestedPlan extends Extractor
{
    /**
     * The plan whose output this source reads; with $limit, that plan stops after $limit rows - a limit pushed into
     * this source reaches the nested plan's own source through its own rules.
     *
     * @param null|positive-int $limit
     */
    public function plan(?int $limit = null): Plan;

    /**
     * The schema declared with withSchema(), or null when this extractor derives its schema.
     *
     * Non-null means the extractor COERCES its output (it re-hydrates every batch against this schema),
     * so the planner must NOT inline its plan - it must let extract() run.
     */
    public function declaredSchema(): ?Schema;
}
