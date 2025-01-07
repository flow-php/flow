<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Extractor, FlowContext, Row, Rows};

final readonly class SequenceExtractor implements Extractor
{
    public function __construct(
        private SequenceGenerator\SequenceGenerator $generator,
        private string $entryName = 'entry',
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        /** @var mixed $item */
        foreach ($this->generator->generate() as $item) {
            $signal = yield new Rows(Row::create($context->entryFactory()->create($this->entryName, $item)));

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
