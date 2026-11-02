<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Optimizer\Rule;

use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\ReplaceNodes;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;
use SplObjectStorage;

final readonly class PushProjectionIntoSource implements Rule
{
    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        /** @var SplObjectStorage<Read, RequiredColumns> $demands */
        $demands = new SplObjectStorage();

        foreach ($plan->consumerInputs() as $input) {
            $required = RequiredColumns::all();

            for ($node = $input; $node->children() !== []; $node = $node->children()[0]) {
                $required = $node->requiredInputColumns($required);
            }

            if ($node instanceof Read) {
                $demands[$node] = $demands->offsetExists($node) ? $demands[$node]->union($required) : $required;
            }
        }

        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();

        foreach ($demands as $read) {
            if ($demands[$read]->allBut || !$read->extractor() instanceof FileExtractor) {
                continue;
            }

            $replacements[$read] = $read->withColumns(...$demands[$read]->names);
        }

        return $replacements->count() === 0 ? $plan : $plan->transformUp(new ReplaceNodes($replacements));
    }
}
