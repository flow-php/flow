<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ScalarFunction\ExpandResults;

final readonly class ExpandingFunctions
{
    /**
     * @return list<ExpandResults>
     */
    public function in(FunctionTree $tree): array
    {
        $found = $tree instanceof ExpandResults ? [$tree] : [];

        foreach ($tree->children() as $child) {
            $found = [...$found, ...$this->in($child)];
        }

        return $found;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function refuse(FunctionTree $tree, string $method): void
    {
        if ($this->in($tree) !== []) {
            throw InvalidArgumentException::because(
                '%s() cannot contain array_expand(), it turns one row into many rows. '
                . 'Expand with withEntry() first, then use the new column.',
                $method,
            );
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    public function refuseNested(FunctionTree $tree): void
    {
        foreach ($this->in($tree) as $expand) {
            foreach ($expand->children() as $child) {
                if ($this->in($child) !== []) {
                    throw InvalidArgumentException::because(
                        'array_expand() cannot contain another array_expand(). Expand one level per withEntry().',
                    );
                }
            }
        }
    }
}
