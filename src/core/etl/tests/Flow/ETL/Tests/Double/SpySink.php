<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\DataFrame;
use Flow\ETL\Sink;

/**
 * @import-type Sinks from \Flow\ETL\Plan\LogicalPlan
 */
final class SpySink implements Sink
{
    public ?DataFrame $prefix = null;

    /**
     * @return Sinks
     */
    public function roots(DataFrame $prefix): array
    {
        $this->prefix = $prefix;

        return [];
    }
}
