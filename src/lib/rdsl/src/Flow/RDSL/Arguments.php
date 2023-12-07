<?php declare(strict_types=1);

namespace Flow\RDSL;

final class Arguments
{
    /**
     * @param array<mixed> $arguments
     */
    public function __construct(private readonly array $arguments = [])
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
