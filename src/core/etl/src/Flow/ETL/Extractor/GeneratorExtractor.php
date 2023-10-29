<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

/**
 * @internal
 */
final class GeneratorExtractor implements Extractor
{
    /**
     * @param \Generator<Rows> $rows
     */
    public function __construct(private readonly \Generator $rows)
    {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->rows as $row) {
            if (!$row instanceof Rows) {
                throw new InvalidArgumentException('Passed generator can contain only Rows class instances, given: ' . $row::class);
            }

            yield $row;
        }
    }
}
