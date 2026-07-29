<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Window;
use Flow\ETL\Window\WindowContext;

interface WindowFunction
{
    public function apply(WindowContext $window): mixed;

    public function over(Window $window): static;

    public function toString(): string;

    public function window(): Window;
}
