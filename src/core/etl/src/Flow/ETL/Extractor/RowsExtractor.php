<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

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

    public function extract(FlowContext $context): Generator
    {
        foreach ($this->rows as $rows) {
            // @mago-ignore analysis:mixed-assignment
            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
