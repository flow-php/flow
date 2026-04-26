<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Unit\Double;

use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;

final class TestableSessionHandler extends FlowPostgreSqlSessionHandler
{
    public function exposedDoDestroy(string $sessionId) : bool
    {
        return $this->doDestroy($sessionId);
    }

    public function exposedDoRead(string $sessionId) : string
    {
        return $this->doRead($sessionId);
    }

    public function exposedDoWrite(string $sessionId, string $data) : bool
    {
        return $this->doWrite($sessionId, $data);
    }
}
