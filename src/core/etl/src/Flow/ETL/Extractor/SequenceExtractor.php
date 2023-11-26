<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class SequenceExtractor implements Extractor
{
    public function __construct(
        private readonly SequenceGenerator\SequenceGenerator $generator,
        private readonly string $entryName = 'entry',
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        /** @var mixed $item */
        foreach ($this->generator->generate() as $item) {
            $signal = yield new Rows(Row::create($context->entryFactory()->create($this->entryName, $item, $context->schema())));

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
