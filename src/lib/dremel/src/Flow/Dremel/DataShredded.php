<?php

declare(strict_types=1);

namespace Flow\Dremel;

final class DataShredded
{
    public function __construct(
        public readonly array $repetitions,
        public readonly array $definitions,
        public readonly array $values
    ) {
    }
}
