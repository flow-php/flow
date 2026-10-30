<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use Flow\ETL\Values\Rows;

/**
 * Not a Loader: it is never described, and it returns its outcome to the runner instead of throwing it.
 */
interface NestedLoader extends Closure, Discardable
{
    /**
     * @return LoadingError|TransformationError|null the failure no handler has decided yet, for the caller to offer
     *                                                once; null when written. A thrown failure surfaces as it is: a
     *                                                handler decided it, or no user step raised it.
     */
    public function load(Rows $rows): LoadingError|TransformationError|null;
}
