<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use SensitiveParameter;

use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;
use function range;

final class AddRowIndexTransformer implements Stateful, Transformer
{
    private int $index;

    public function __construct(
        private readonly string $indexColumn,
        private readonly StartFrom $startFrom,
    ) {
        $this->index = $startFrom === StartFrom::ZERO ? 0 : 1;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            type_string()->assert($data['index_column'] ?? null),
            type_enum(StartFrom::class)->assert(constant(
                StartFrom::class . '::' . type_string()->assert($data['start_from'] ?? null),
            )),
        );
    }

    public function describe(): ?Description
    {
        return new Description(self::class, [
            'index_column' => $this->indexColumn,
            'start_from' => $this->startFrom->name,
        ]);
    }

    public function fresh(): static
    {
        return new self($this->indexColumn, $this->startFrom);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input->add(new IntegerDefinition($this->indexColumn)));
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $column = $backend->builder(new IntegerDefinition($this->indexColumn));

        if ($rows->count() > 0) {
            $column->appendMany(range($this->index, $this->index + $rows->count() - 1));
            $this->index += $rows->count();
        }

        return $rows->withColumns($rows->schema()->add(new IntegerDefinition($this->indexColumn)), [
            $this->indexColumn => $column->finish(),
        ]);
    }
}
