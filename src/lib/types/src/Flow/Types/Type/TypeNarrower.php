<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Type;

interface TypeNarrower
{
    /**
     * @return Type<mixed>
     */
    public function narrow(mixed $value) : Type;
}
