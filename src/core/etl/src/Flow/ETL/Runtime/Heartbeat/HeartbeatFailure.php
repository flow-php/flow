<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

enum HeartbeatFailure
{
    case stop;
    case log;
}
