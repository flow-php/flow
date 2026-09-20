<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;

/**
 * Enumerates the spine's pipelines only - a sink root's sink pipeline hangs off the SinkFeed that feeds it.
 *
 * @inheritors Described|Raw
 */
interface PhysicalPlan
{
    public function root(): Pipeline;

    /**
     * @throws SchemaNotDerivableException
     */
    public function schema(): Schema;
}
