<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * Enumerates the spine's pipelines only - a sink root's side pipeline hangs off the SinkFeed that feeds it.
 *
 * @inheritors Plan\Described|Plan\Raw
 */
interface Plan
{
    public function root(): Plan\Pipeline;
}
