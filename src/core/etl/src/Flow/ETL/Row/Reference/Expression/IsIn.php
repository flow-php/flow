<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\ValueExtractor;

final class IsIn implements Expression
{
    public function __construct(
        private readonly Expression $haystack,
        private readonly Expression $needle
    ) {
    }

    /**
     * @psalm-suppress MixedAssignment
     */
    public function eval(Row $row) : bool
    {
        $extractor = new ValueExtractor();
        $haystack = $extractor->value($row, $this->haystack);
        $needle = $extractor->value($row, $this->needle);

        if (!\is_array($haystack)) {
            return false;
        }

        return \in_array($needle, $haystack, true);
    }
}
