<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\ValueExtractor;

final class Divide implements Expression
{
    public function __construct(
        private readonly Expression $leftRef,
        private readonly Expression $rightRef
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $extractor = new ValueExtractor();
        $left = $extractor->value($row, $this->leftRef, 0);
        $right = $extractor->value($row, $this->rightRef, 0);

        if (!\is_numeric($left) || !\is_numeric($right)) {
            return null;
        }

        if ($right === 0) {
            return null;
        }

        return $left / $right;
    }
}
