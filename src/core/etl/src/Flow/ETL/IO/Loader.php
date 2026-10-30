<?php

declare(strict_types=1);

namespace Flow\ETL\IO;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;

interface Loader extends Describable
{
    public function load(Rows $rows, Backend $backend): void;
}
