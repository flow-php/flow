<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;

/**
 * Every column a function tree reads, by SOURCE name (Reference::to()), so an alias cannot smuggle a
 * different column past a membership check.
 */
final readonly class ReferencedColumns
{
    public function in(FunctionTree $tree): References
    {
        $refs = References::init();

        if ($tree instanceof Reference) {
            $refs = $refs->add($tree->to());
        }

        foreach ($tree->children() as $child) {
            foreach ($this->in($child)->all() as $ref) {
                $refs = $refs->add($ref->to());
            }
        }

        return $refs;
    }
}
