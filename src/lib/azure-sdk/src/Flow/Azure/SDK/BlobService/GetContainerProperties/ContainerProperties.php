<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetContainerProperties;

use Flow\Azure\SDK\Exception\InvalidArgumentException;
use Psr\Http\Message\ResponseInterface;

final class ContainerProperties
{
    public function __construct(private readonly ResponseInterface $response)
    {
        if ($this->response->getStatusCode() < 200 || $this->response->getStatusCode() >= 300) {
            throw new InvalidArgumentException('Container properties could not be fetched');
        }
    }
}
