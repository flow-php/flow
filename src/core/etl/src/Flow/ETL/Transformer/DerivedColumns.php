<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;

final readonly class DerivedColumns
{
    /**
     * @param Definition<mixed> $derived
     */
    public function declare(Schema $input, Definition $derived): Schema
    {
        $name = $derived->entry()->name();

        return $input->findDefinition($name) === null ? $input->add($derived) : $input->replace($name, $derived);
    }

    /**
     * @param Definition<mixed> $derived
     *
     * @throws SchemaMismatchException
     */
    public function value(Definition $derived, mixed $value, int $rowIndex): mixed
    {
        // @mago-ignore analysis:mixed-assignment
        $cast = $value === null ? null : $derived->type()->cast($value);

        if (!$derived->matches($cast)) {
            throw new SchemaMismatchException($rowIndex, ColumnMismatchException::valueDoesNotMatch($derived, $cast));
        }

        return $cast;
    }

    public function rows(Rows $input, Schema $declared, Schema $output, string $name, Column $derived): Rows
    {
        $withDerived = $input->withColumns($declared, [$name => $derived]);

        return $declared->isSame($output) ? $withDerived : $withDerived->matchTo($output);
    }
}
