<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\ValueExtractor;

final class Value implements Expression
{
    public function __construct(private readonly string|Expression $entry)
    {
    }

    public function eval(Row $row) : mixed
    {
        if ($this->entry instanceof Literal) {
            return $this->entry->value();
        }

        if (\is_string($this->entry)) {
            return (new ValueExtractor())->value($row, ref($this->entry));
        }

        return (new ValueExtractor())->value($row, $this->entry);
    }
}
