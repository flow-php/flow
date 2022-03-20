<?php declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

interface Aggregator
{
    public function aggregate(Row $row) : void;

    public function result() : Entry;
}
