<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

interface Heartbeat
{
    public function beat(Pulse $pulse): void;
}
