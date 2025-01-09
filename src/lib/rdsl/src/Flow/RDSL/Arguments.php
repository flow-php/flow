<?php

declare(strict_types=1);

namespace Flow\RDSL;

final readonly class Arguments
{
    /**
     * @param array<mixed> $arguments
     */
    public function __construct(private array $arguments = [])
    {
    }

    /**
     * @return array<mixed>
     */
    public function toArray() : array
    {
        return $this->arguments;
    }
}
