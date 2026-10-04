<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Schema;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;

final readonly class FunctionContext
{
    public FlowContext $context;

    public function __construct(?FlowContext $context = null)
    {
        $this->context = $context ?? flow_context();
    }

    /**
     * @param list<array<array-key, mixed>> $rows
     */
    public function column(ScalarFunction $function, array $rows, Schema $schema): Column
    {
        return (new ReferenceResolver())
            ->resolve($function, $schema)
            ->eval(array_to_rows($rows, $schema, $this->context->backend()), $this->context);
    }

    /**
     * Resolves $function against $schema (steps do the same), evaluates one row, returns its value as a parent
     * function reads it.
     *
     * @param array<array-key, mixed> $row
     */
    public function eval(ScalarFunction $function, array $row, Schema $schema): mixed
    {
        return (new Parameter((new ReferenceResolver())->resolve($function, $schema)))->values(
            array_to_rows([$row], $schema, $this->context->backend()),
            $this->context,
        )[0];
    }
}
