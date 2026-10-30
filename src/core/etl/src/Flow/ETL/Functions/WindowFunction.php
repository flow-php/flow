<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;

interface WindowFunction extends FunctionTree, Describable
{
    /**
     * One value per row of $partition, which is sorted by the window's ORDER BY.
     *
     * @return list<mixed>
     */
    public function values(Rows $partition, Backend $backend): array;

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
