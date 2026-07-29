<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Rows;

final readonly class WholePartitionFrame implements WindowFrame
{
    public function bounds(int $index, Rows $partition): array
    {
        return [0, $partition->count() - 1];
    }
}
