<?php

declare(strict_types=1);

namespace Flow\ETL\Time;

use function usleep;

final class SystemSleep implements Sleep
{
    public function for(Duration $duration): void
    {
        usleep($duration->microseconds());
    }
}
