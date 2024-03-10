<?php

declare(strict_types=1);

namespace Flow\Dremel;

interface Node
{
    public function value() : ?array;
}
