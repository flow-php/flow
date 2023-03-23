<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\ValueExtractor;

final class IsNotNull implements Expression
{
    public function __construct(
        private readonly EntryReference|Literal $ref
    ) {
    }

    public function eval(Row $row) : bool
    {
        return (new ValueExtractor())->value($row, $this->ref) !== null;
    }
}
