<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlob;

use Flow\Azure\SDK\Exception\Exception;
use Psr\Http\Message\ResponseInterface;

final readonly class BlobContent
{
    public function __construct(private ResponseInterface $response)
    {
        if ($this->response->getStatusCode() < 200 || $this->response->getStatusCode() >= 300) {
            throw new \RuntimeException('Blob content could not be fetched');
        }
    }

    public function content() : string
    {
        return $this->response->getBody()->getContents();
    }

    public function length() : int
    {
        return (int) $this->response->getHeaderLine('Content-Length');
    }

    /**
     * @return resource
     */
    public function stream()
    {
        $stream = $this->response->getBody()->detach();

        if (!\is_resource($stream)) {
            throw new Exception('Blob content stream could not be accessed');
        }

        return $stream;
    }
}
