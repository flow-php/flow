<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

final readonly class GeneratorExtractor implements Extractor
{
    /**
     * @param \Generator<Rows> $rows
     */
    public function __construct(
        private Generator $rows,
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($this->rows as $row) {
            // @mago-ignore analysis:impossible-condition
            if (!$row instanceof Rows) {
                // @mago-ignore analysis:invalid-class-string-expression,invalid-operand
                throw new InvalidArgumentException('Passed generator can contain only Rows class instances, given: '
                . $row::class);
            }

            $signal = yield $row;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
