<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Extractor, FlowContext, Rows};

/**
 * @internal
 */
final readonly class RowsExtractor implements Extractor
{
    /**
     * @var array<Rows>
     */
    private array $rows;

    public function __construct(Rows ...$rows)
    {
        $this->rows = $rows;
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->rows as $rows) {
            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
