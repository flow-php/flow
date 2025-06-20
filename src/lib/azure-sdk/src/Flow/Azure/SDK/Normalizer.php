<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

interface Normalizer
{
    /**
     * @return array<array-key, mixed>
     */
    public function toArray(string $data) : array;
}
