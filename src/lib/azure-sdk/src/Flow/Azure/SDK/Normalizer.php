<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

interface Normalizer
{
    public function toArray(string $data) : array;
}
