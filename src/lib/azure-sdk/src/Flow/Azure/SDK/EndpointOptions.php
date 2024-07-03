<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

interface EndpointOptions
{
    public function toHeaders() : array;

    public function toURIParameters() : array;
}
