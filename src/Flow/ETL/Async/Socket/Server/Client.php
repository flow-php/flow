<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Server;

use Flow\ETL\Async\Socket\Communication\Message;

interface Client
{
    public function disconnect() : void;

    public function send(Message $message) : void;
}
