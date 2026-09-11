<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Window;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

interface WindowFunction extends FunctionTree
{
    public function apply(WindowContext $window): mixed;

    /**
     * Returns a copy bound to $window. Never mutates $this.
     */
    public function over(Window $window): static;

    /**
     * An OptionalType return declares the produced column nullable; there is no nullable() peer.
     * The frame contributes nothing to the type - Window is a field, not a parameter.
     *
     * @return Type<mixed>
     */
    public function returns(): Type;

    public function toString(): string;

    public function window(): Window;
}
