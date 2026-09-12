<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;

/**
 * @internal
 */
final readonly class BoundWindow
{
    /**
     * @param Definition<mixed> $derived
     */
    public function __construct(
        public WindowFunction $resolved,
        public Definition $derived,
        public Schema $input,
        public Schema $output,
    ) {}
}
