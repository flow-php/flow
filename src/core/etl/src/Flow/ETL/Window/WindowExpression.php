<?php declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Window;

final class WindowExpression
{
    private ?Window $window = null;

    public function __construct(private readonly WindowFunction $function)
    {

    }

    public function function() : WindowFunction
    {
        return $this->function;
    }

    public function over(Window $window) : self
    {
        $this->window = $window;

        return $this;
    }

    public function window() : Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->function->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
