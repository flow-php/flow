<?php

declare(strict_types=1);

namespace Flow\ETL\Time;

use function max;
use function usleep;

final class SystemSleep implements Sleep
{
    public function for(Duration $duration): void
    {
        usleep(max(0, $duration->microseconds()));
    }
}
