<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row;
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

    /**
     * @param list<Row> $rows each carrying a derived column checked by value()
     */
    public function rows(Schema $declared, Schema $output, array $rows): Rows
    {
        return $declared->isSame($output) ? Rows::trusted($output, $rows) : new Rows($output, ...$rows);
    }
}
