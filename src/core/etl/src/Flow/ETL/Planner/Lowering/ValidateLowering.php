<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Validate;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<Validate>
 */
final readonly class ValidateLowering implements Lowering
{
    /**
     * @return class-string<Validate>
     */
    public function handles(): string
    {
        return Validate::class;
    }

    /**
     * @param Validate $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [new SchemaValidationLoader($node->schema, $node->validator)];
    }
}
