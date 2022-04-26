<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Communication;

final class Protocol
{
    public const CLIENT_FETCH = 'fetch';

    public const CLIENT_IDENTIFY = 'identify';

    public const SERVER_PROCESS = 'process';

    public const SERVER_SETUP = 'pipes';
}
