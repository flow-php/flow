<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Fixtures\Join;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Row;

final readonly class AlwaysMeets implements Comparison
{
    public function compare(Row $left, Row $right): bool
    {
        return true;
    }

    public function left(): array
    {
        return [];
    }

    public function right(): array
    {
        return [];
    }
}
