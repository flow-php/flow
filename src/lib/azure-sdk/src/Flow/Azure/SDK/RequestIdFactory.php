<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

use Psr\Http\Message\RequestInterface;

interface RequestIdFactory
{
    public function for(RequestInterface $request) : string;
}
