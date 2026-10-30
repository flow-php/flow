<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Loader;

use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

use function array_key_exists;
use function is_array;

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
            ->toFlowSchema($schema)
            ->definitions() as $definition) {
            $definitions[$definition->name()] = $definition;
        }

        $this->definitions = $definitions;
    }

    public function conform(Rows $rows, Backend $backend): Rows
    {
        $columns = $rows->columns();
        $definitions = [];
        $cast = false;

        foreach ($rows->schema()->definitions() as $key => $own) {
            $writer = $this->definitions[$own->name()] ?? null;

            if (
                $writer === null
                || self::storedType($writer->type()->normalize()) === self::storedType($own->type()->normalize())
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

    /**
     * The type as a Parquet file stores it: a datetime's zone is not stored.
     *
     * @param array<array-key, mixed> $normalized Type::normalize()
     *
     * @return array<array-key, mixed>
     */
    public static function storedType(array $normalized): array
    {
        if (array_key_exists('type', $normalized) && $normalized['type'] === 'datetime') {
            unset($normalized['zone']);
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($normalized as $key => $value) {
            if (is_array($value)) {
                $normalized[$key] = self::storedType($value);
            }
        }

        return $normalized;
    }
}
