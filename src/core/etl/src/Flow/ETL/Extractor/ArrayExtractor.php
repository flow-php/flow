<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\{Extractor, FlowContext, Row\Schema};

final class ArrayExtractor implements Extractor
{
    /**
     * @param iterable<array<mixed>> $dataset
     */
    public function __construct(private readonly iterable $dataset, private readonly ?Schema $schema = null)
    {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->dataset as $row) {
            yield array_to_rows([$row], $context->entryFactory(), [], $this->schema);
        }
    }
}
