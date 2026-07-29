<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Window;
use Flow\ETL\Window\WindowContext;

final class RowNumber implements WindowFunction
{
    private ?Window $window;

    public function __construct()
    {
        $this->window = null;
    }

    public function apply(WindowContext $window): mixed
    {
        return $window->index() + 1;
    }

    public function over(Window $window): static
    {
        $this->window = $window;

        return $this;
    }

    public function toString(): string
    {
        return 'row_number()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
