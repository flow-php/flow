<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Functions\Parameter;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Throwable;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function var_export;

final readonly class FunctionContext
{
    public FlowContext $context;

    public function __construct(?FlowContext $context = null)
    {
        $this->context = $context ?? flow_context();
    }

    /**
     * The var_export() text of $function's column over $rows, or the failure its evaluation raised: what a rebuilt
     * function must reproduce exactly.
     */
    public function exported(ScalarFunction $function, Rows $rows, Schema $schema): string
    {
        try {
            return var_export(
                (new ReferenceResolver())
                    ->resolve($function, $schema)
                    ->eval($rows, $this->context->backend())
                    ->values(),
                true,
            );
        } catch (Throwable $e) {
            return $e::class . ': ' . $e->getMessage();
        }
    }

    /**
     * $function's column over $rows, resolved against $schema.
     */
    public function columnOf(ScalarFunction $function, Rows $rows, Schema $schema): Column
    {
        return (new ReferenceResolver())
            ->resolve($function, $schema)
            ->eval($rows, $this->context->backend());
    }

    /**
     * @param list<array<array-key, mixed>> $rows
     */
    public function column(ScalarFunction $function, array $rows, Schema $schema): Column
    {
        return (new ReferenceResolver())
            ->resolve($function, $schema)
            ->eval(array_to_rows($rows, $schema, $this->context->backend()), $this->context->backend());
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
            $this->context->backend(),
        )[0];
    }
}
