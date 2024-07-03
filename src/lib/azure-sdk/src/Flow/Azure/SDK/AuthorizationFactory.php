<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

use Psr\Http\Message\RequestInterface;

interface AuthorizationFactory
{
    public function for(RequestInterface $request) : string;
}
