<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Rows\ColumnRetyping;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;

use function Flow\Types\DSL\type_bare;

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
     * The function's column as the stored derived column: NOT NULL enforced here (intermediates are nullable), the
     * same type adopted into the configured backend, any other type rebuilt through it.
     *
     * @param Definition<mixed> $derived
     *
     * @throws SchemaMismatchException
     */
    public function stored(Definition $derived, Column $column, Backend $backend): Column
    {
        (new ColumnRetyping())->notNull($derived, $column);

        if (type_bare($column->type())->normalize() === type_bare($derived->type())->normalize()) {
            return $backend->adopt($derived, $column->withType($derived->type()));
        }

        $builder = $backend->builder($derived);
        $builder->appendMany($column->values());

        return $builder->finish();
    }

    public function rows(
        Rows $input,
        Schema $declared,
        Schema $output,
        string $name,
        Column $derived,
        Backend $backend,
    ): Rows {
        $withDerived = $input->withColumns($declared, [$name => $derived]);

        return $declared->isSame($output) ? $withDerived : $withDerived->matchTo($output, $backend);
    }
}
