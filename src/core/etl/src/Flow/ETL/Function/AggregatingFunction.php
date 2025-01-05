<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

interface AggregatingFunction
{
    public function aggregate(Row $row) : void;

    /**
     * @return Entry<mixed, mixed>
     */
    public function result() : Entry;
}
