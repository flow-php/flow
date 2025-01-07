<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor, FlowContext, Rows};

/**
 * @internal
 */
final readonly class GeneratorExtractor implements Extractor
{
    /**
     * @param \Generator<Rows> $rows
     */
    public function __construct(private \Generator $rows)
    {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->rows as $row) {
            if (!$row instanceof Rows) {
                throw new InvalidArgumentException('Passed generator can contain only Rows class instances, given: ' . $row::class);
            }

            $signal = yield $row;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
