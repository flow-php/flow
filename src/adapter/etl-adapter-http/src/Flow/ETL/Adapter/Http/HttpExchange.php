<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final readonly class HttpExchange
{
    public function __construct(
        public RequestInterface $request,
        public ResponseInterface $response,
    ) {}
}
