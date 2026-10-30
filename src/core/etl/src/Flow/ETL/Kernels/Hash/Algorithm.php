<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Hash;

interface Algorithm
{
    public function hash(string $value): string;
}
