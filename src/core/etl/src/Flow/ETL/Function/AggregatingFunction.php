<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;

interface AggregatingFunction
{
    public function aggregate(Row $row, FlowContext $context): void;

    /**
     * @return null|list<Reference> references this aggregator reads, or null when they cannot be
     *                              statically enumerated (disables spill column pruning)
     */
    public function references(): ?array;

    /**
     * @return Entry<mixed>
     */
    public function result(EntryFactory $entryFactory): Entry;
}
