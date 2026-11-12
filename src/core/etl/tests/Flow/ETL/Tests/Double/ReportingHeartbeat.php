<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Progress;
use Flow\ETL\Runtime\Heartbeat\Heartbeat;
use Flow\ETL\Runtime\Heartbeat\Pulse;

final class ReportingHeartbeat implements Heartbeat
{
    public int $beats = 0;

    public ?Progress $reportsTo = null;

    public function beat(Pulse $pulse): void
    {
        $this->beats++;

        // only the first beat reports, so a throttle that beats again inside a beat ends instead of recursing
        if ($this->beats === 1) {
            $this->reportsTo?->advanced(1);
        }
    }
}
