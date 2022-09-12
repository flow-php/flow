<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

/**
 * @internal
 *
 * @psalm-immutable
 */
final class ProcessExtractor implements Extractor
{
    /**
     * @var array<Rows>
     */
    private readonly array $rows;

    public function __construct(Rows ...$rows)
    {
        $this->rows = $rows;
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->rows as $rows) {
            yield $rows;
        }
    }
}
