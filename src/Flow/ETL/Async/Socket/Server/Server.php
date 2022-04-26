<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Server;

/**
 * @internal
 */
interface Server
{
    public function host() : string;

    public function initialize(ServerProtocol $handler) : void;

    public function isRunning() : bool;

    public function start() : void;

    public function stop() : void;
}
