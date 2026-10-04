<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

final readonly class ParquetSchemaConformance
{
    /**
     * @var array<string, Definition<mixed>>
     */
    private array $definitions;

    public function __construct(ParquetSchema $schema)
    {
        $definitions = [];

        foreach ((new SchemaConverter())
            ->toFlow($schema)
            ->definitions() as $definition) {
            $definitions[$definition->entry()->name()] = $definition;
        }

        $this->definitions = $definitions;
    }

    public function conform(Rows $rows, Backend $backend): Rows
    {
        $columns = $rows->columns();
        $definitions = [];
        $cast = false;

        foreach ($rows->schema()->definitions() as $key => $own) {
            $writer = $this->definitions[$own->entry()->name()] ?? null;

            if (
                $writer === null
                || ParquetStoredType::of($writer->type()->normalize()) === ParquetStoredType::of(
                    $own->type()->normalize(),
                )
            ) {
                $definitions[] = $own;

                continue;
            }

            $builder = $backend->builder($writer);
            $builder->appendMany($columns[$key]->values());
            $columns[$key] = $builder->finish();
            $definitions[] = $writer;
            $cast = true;
        }

        return $cast ? Rows::fromColumns(new Schema(...$definitions), $columns, $rows->count()) : $rows;
    }
}
