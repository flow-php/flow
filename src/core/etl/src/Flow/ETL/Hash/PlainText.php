<?php

declare(strict_types=1);

namespace Flow\ETL\Hash;

final class PlainText implements Algorithm
{
    public function hash(string $value) : string
    {
        return $value;
    }
}
