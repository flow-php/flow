<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\ValueExtractor;

final class NotSame implements Expression
{
    public function __construct(
        private readonly Expression $base,
        private readonly Expression $next
    ) {
    }

    /**
     * @psalm-suppress MixedAssignment
     */
    public function eval(Row $row) : bool
    {
        $extractor = new ValueExtractor();
        $base = $extractor->value($row, $this->base);
        $next = $extractor->value($row, $this->next);

        return $base !== $next;
    }
}
