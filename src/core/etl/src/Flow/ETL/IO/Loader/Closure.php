<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader;

use Flow\ETL\Foundation\Progress;

interface Closure
{
    public function closure(Progress $progress): void;
}
