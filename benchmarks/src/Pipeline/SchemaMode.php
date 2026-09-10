<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

enum SchemaMode
{
    case declared;
    case inferred;
}
