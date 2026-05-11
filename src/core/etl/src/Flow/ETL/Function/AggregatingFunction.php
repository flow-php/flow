<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;

interface AggregatingFunction
{
    public function aggregate(Row $row, FlowContext $context): void;

    /**
     * @return Entry<mixed>
     */
    public function result(EntryFactory $entryFactory): Entry;
}
