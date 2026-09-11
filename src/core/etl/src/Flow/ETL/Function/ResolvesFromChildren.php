<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

/**
 * @require-implements FunctionTree
 */
trait ResolvesFromChildren
{
    public function resolved(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->resolved()) {
                return false;
            }
        }

        return true;
    }
}
