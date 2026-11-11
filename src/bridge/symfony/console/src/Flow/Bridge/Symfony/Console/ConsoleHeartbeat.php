<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

use Flow\ETL\Runtime\Heartbeat\Heartbeat;

interface ConsoleHeartbeat extends Heartbeat
{
    public const int MAX_WIDTH = 100;

    public const array SPINNER = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

    public function finish(): void;
}
