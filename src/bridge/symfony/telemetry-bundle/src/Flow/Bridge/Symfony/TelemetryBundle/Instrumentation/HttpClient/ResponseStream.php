<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient;

use Generator;
use Symfony\Contracts\HttpClient\ChunkInterface;
use Symfony\Contracts\HttpClient\ResponseInterface;
use Symfony\Contracts\HttpClient\ResponseStreamInterface;

final readonly class ResponseStream implements ResponseStreamInterface
{
    /**
     * @param Generator<ResponseInterface, ChunkInterface> $generator
     */
    public function __construct(
        private Generator $generator,
    ) {}

    public function current(): ChunkInterface
    {
        return $this->generator->current();
    }

    public function key(): ResponseInterface
    {
        return $this->generator->key();
    }

    public function next(): void
    {
        $this->generator->next();
    }

    public function rewind(): void
    {
        $this->generator->rewind();
    }

    public function valid(): bool
    {
        return $this->generator->valid();
    }
}
