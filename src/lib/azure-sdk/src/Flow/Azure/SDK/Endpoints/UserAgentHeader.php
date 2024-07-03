<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Endpoints;

trait UserAgentHeader
{
    private string $userAgentHeader = 'flow-php/azure-sdk';

    public function userAgentHeader() : string
    {
        return $this->userAgentHeader;
    }

    public function withUserAgent(string $userAgentHeader) : void
    {
        $this->userAgentHeader = $userAgentHeader;
    }
}
