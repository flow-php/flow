<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\ValueExtractor;

final class Power implements Expression
{
    public function __construct(
        private readonly EntryReference|Literal $leftRef,
        private readonly EntryReference|Literal $rightRef
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $extractor = new ValueExtractor();
        $left = $extractor->value($row, $this->leftRef, 0);
        $right = $extractor->value($row, $this->rightRef, 0);

        if ($right === 0) {
            return null;
        }

        if (!\is_numeric($left) || !\is_numeric($right)) {
            return null;
        }

        return $left ** $right;
    }
}
