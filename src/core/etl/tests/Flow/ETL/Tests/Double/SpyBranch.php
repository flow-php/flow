<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\API\Branch;
use Flow\ETL\API\DataFrame;

final class SpyBranch implements Branch
{
    public ?DataFrame $prefix = null;

    public function write(DataFrame $prefix): DataFrame
    {
        $this->prefix = $prefix;

        return $prefix;
    }
}
