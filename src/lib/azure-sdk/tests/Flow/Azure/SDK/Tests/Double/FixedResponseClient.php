<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Double;

use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use RuntimeException;

final class FixedResponseClient implements ClientInterface
{
    /** @var array<int, RequestInterface> */
    private array $requests = [];

    /** @var array<int, ResponseInterface> */
    private array $responses;

    public function __construct(ResponseInterface ...$responses)
    {
        $this->responses = array_values($responses);
    }

    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        $this->requests[] = $request;

        $response = array_shift($this->responses);

        if ($response === null) {
            throw new RuntimeException('FixedResponseClient: no more responses queued');
        }

        return $response;
    }

    /**
     * @return array<int, RequestInterface>
     */
    public function requests(): array
    {
        return $this->requests;
    }
}
