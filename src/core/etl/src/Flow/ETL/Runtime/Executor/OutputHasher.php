<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Runtime\Metrics\DatasetHash;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;

interface OutputHasher extends Describable
{
    public function add(Rows $rows, Backend $backend): void;

    public function absorb(DatasetHash $partial): void;

    public function hash(): DatasetHash;
}
