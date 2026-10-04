<?php

declare(strict_types=1);

namespace Flow\ETL\Function\ScalarFunction;

use Flow\ETL\Function\ScalarFunction;

/**
 * eval returns a `list<returns()>` column; the step explodes it.
 */
interface ExpandResults extends ScalarFunction {}
