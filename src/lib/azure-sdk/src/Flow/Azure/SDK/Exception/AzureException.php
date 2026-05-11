<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Exception;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class AzureException extends Exception
{
    public function __construct(
        string $endpoint,
        public readonly RequestInterface $request,
        public readonly ResponseInterface $response,
    ) {
        parent::__construct(\sprintf(
            'Azure SDK Exception: %s, %d, %s',
            $endpoint,
            $this->response->getStatusCode(),
            $this->response->getBody()->getContents(),
        ));
    }
}
