<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\DataFrame;
use Flow\ETL\Sink;

final class SpySink implements Sink
{
    public ?DataFrame $prefix = null;

    public function write(DataFrame $prefix): void
    {
        $this->prefix = $prefix;
    }
}
