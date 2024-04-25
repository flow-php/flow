<?php

declare(strict_types=1);

namespace Flow\ETL\Hash;

final class NativePHPHash implements Algorithm
{
    public function __construct(private string $algorithm = 'xxh128', private bool $binary = false, private array $options = [])
    {
        if (!\in_array($algorithm, \hash_algos(), true)) {
            throw new \InvalidArgumentException(\sprintf('Hashing algorithm "%s" is not supported', $algorithm));
        }
    }

    public function hash(string $value) : string
    {
        return \hash($this->algorithm, $value, $this->binary, $this->options);
    }
}
