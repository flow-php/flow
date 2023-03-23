<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\ValueExtractor;

final class StartsWith implements Expression
{
    public function __construct(private readonly EntryReference $haystack, private readonly EntryReference $needle)
    {
    }

    public function eval(Row $row) : bool
    {
        $extractor = new ValueExtractor();
        $haystack = $extractor->value($row, $this->haystack);
        $needle = $extractor->value($row, $this->needle);

        if (!\is_string($needle) || !\is_string($haystack)) {
            return false;
        }

        return \str_starts_with($haystack, $needle);
    }
}
