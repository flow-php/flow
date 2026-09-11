<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;

/**
 * @type ForeignKeyShape = array{name: ?string, columns: non-empty-list<string>, reference_schema: string, reference_table: string, reference_columns: non-empty-list<string>, on_update?: string, on_delete?: string, deferrable?: bool, initially_deferred?: bool}
 */
final readonly class ForeignKey
{
    /**
     * @param non-empty-list<string> $columns
     * @param non-empty-list<string> $referenceColumns
     */
    public function __construct(
        public ?string $name,
        public array $columns,
        public string $referenceSchema,
        public string $referenceTable,
        public array $referenceColumns,
        public ReferentialAction $onUpdate = ReferentialAction::NO_ACTION,
        public ReferentialAction $onDelete = ReferentialAction::NO_ACTION,
        public bool $deferrable = false,
        public bool $initiallyDeferred = false,
    ) {}

    /**
     * @param ForeignKeyShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            columns: $data['columns'],
            referenceSchema: $data['reference_schema'],
            referenceTable: $data['reference_table'],
            referenceColumns: $data['reference_columns'],
            onUpdate: array_key_exists('on_update', $data)
                ? ReferentialAction::from($data['on_update'])
                : ReferentialAction::NO_ACTION,
            onDelete: array_key_exists('on_delete', $data)
                ? ReferentialAction::from($data['on_delete'])
                : ReferentialAction::NO_ACTION,
            deferrable: $data['deferrable'] ?? false,
            initiallyDeferred: $data['initially_deferred'] ?? false,
        );
    }

    public function isEqual(self $other): bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other): bool
    {
        return (
            $this->columns === $other->columns
            && $this->referenceSchema === $other->referenceSchema
            && $this->referenceTable === $other->referenceTable
            && $this->referenceColumns === $other->referenceColumns
            && $this->onUpdate === $other->onUpdate
            && $this->onDelete === $other->onDelete
            && $this->deferrable === $other->deferrable
            && $this->initiallyDeferred === $other->initiallyDeferred
        );
    }

    /**
     * @return ForeignKeyShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'columns' => $this->columns,
            'reference_schema' => $this->referenceSchema,
            'reference_table' => $this->referenceTable,
            'reference_columns' => $this->referenceColumns,
            'on_update' => $this->onUpdate->value,
            'on_delete' => $this->onDelete->value,
            'deferrable' => $this->deferrable,
            'initially_deferred' => $this->initiallyDeferred,
        ];
    }
}
