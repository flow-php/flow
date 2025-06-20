<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

interface EndpointOptions
{
    /**
     * @return array<string, string>
     */
    public function toHeaders() : array;

    /**
     * @return array<string, string>
     */
    public function toURIParameters() : array;
}
