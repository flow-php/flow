<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Row\{Entry, EntryFactory};

interface AggregatingFunction
{
    public function aggregate(Row $row, FlowContext $context) : void;

    /**
     * @return Entry<mixed>
     */
    public function result(EntryFactory $entryFactory) : Entry;
}
