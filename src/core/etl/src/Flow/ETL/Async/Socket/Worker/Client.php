<?php declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker;

interface Client
{
    public function connect(string $id, string $host, ClientProtocol $protocol) : void;
}
