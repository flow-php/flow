<?php declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker;

use Flow\ETL\Async\Socket\Communication\Message;

interface Server
{
    public function send(Message $message) : void;
}
