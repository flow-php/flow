<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\DynamicExtractor;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

interface NextRequestFactory
{
    /**
     * @psalm-pure
     */
    public function create(?ResponseInterface $previousResponse = null) : ?RequestInterface;
}
