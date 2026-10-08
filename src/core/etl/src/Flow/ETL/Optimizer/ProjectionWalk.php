<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer;

use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Row\References;

final readonly class ProjectionWalk
{
    /**
     * What the node's row input must hold so that $above holds what its consumers read.
     */
    public function below(Node $node, RequiredColumns $above): RequiredColumns
    {
        $refs = new ReferencedColumns();

        return match (true) {
            $node instanceof Node\Select => RequiredColumns::only(...References::init(...$node->entries)->names()),
            $node instanceof Node\Drop => $above->without(...References::init(...$node->entries)->names()),
            // an unpack writes "name.*", known only once bound: keep the demand, add what it reads
            ($node instanceof Node\WithColumn || $node instanceof Node\ExpandColumn)
                && $node->function instanceof UnpackResults
                => $above->with(...$refs->in($node->function)->names()),
            $node instanceof Node\WithColumn, $node instanceof Node\ExpandColumn => $above
                ->without($node->name())
                ->with(...$refs->in($node->function)->names()),
            $node instanceof Node\Filter => $above->with(...$refs->in($node->function)->names()),
            $node instanceof Node\Limit, $node instanceof Node\Offset => $above,
            default => RequiredColumns::all(),
        };
    }
}
