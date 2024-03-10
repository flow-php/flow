<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\DynamicExtractor;

use Psr\Http\Message\{RequestInterface, ResponseInterface};

interface NextRequestFactory
{
    public function create(?ResponseInterface $previousResponse = null) : ?RequestInterface;
}
