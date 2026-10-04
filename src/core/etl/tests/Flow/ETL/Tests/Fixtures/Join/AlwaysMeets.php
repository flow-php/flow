<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Fixtures\Join;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Rows;

final readonly class AlwaysMeets implements Comparison
{
    public function compare(Rows $left, Rows $right): array
    {
        $met = [];

        for ($i = 0, $count = $left->count(); $i < $count; $i++) {
            $met[] = true;
        }

        return $met;
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
