<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Values\Column\Backend;

interface AggregatesByGroup extends AggregatingFunction
{
    public function grouped(Backend $backend): GroupedAggregation;
}
