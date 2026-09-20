<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Plan\Node;

interface JoinsFrame extends Node
{
    /**
     * The joined frame's whole plan - children()[1]. A rewrite of the plan holding this node never reaches into it.
     */
    public function right(): Result|Outputs;
}
