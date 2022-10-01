<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;

/**
 * Loaders implementing OverridingLoader interface overrides one or more loaders.
 * This interface is required by Execution Logical Plan to fully understand execution plan.
 *
 * Examples: TransformerLoader
 */
interface OverridingLoader
{
    /**
     * @return array<Loader>
     */
    public function loaders() : array;
}
