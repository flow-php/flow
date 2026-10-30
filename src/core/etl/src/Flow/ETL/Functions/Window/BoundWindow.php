<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Window;

use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;

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
