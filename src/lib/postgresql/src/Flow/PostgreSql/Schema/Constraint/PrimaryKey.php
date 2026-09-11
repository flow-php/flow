<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

use function sort;

/**
 * @type PrimaryKeyShape = array{columns: non-empty-list<string>, name: ?string}
 */
final readonly class PrimaryKey
{
    /**
     * @param non-empty-list<string> $columns
     */
    public function __construct(
        public array $columns,
        public ?string $name = null,
    ) {}

    /**
     * @param PrimaryKeyShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(columns: $data['columns'], name: $data['name'] ?? null);
    }

    public function isEqual(self $other): bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other): bool
    {
        $aCols = $this->columns;
        $bCols = $other->columns;
        sort($aCols);
        sort($bCols);

        return $aCols === $bCols;
    }

    /**
     * @return PrimaryKeyShape
     */
    public function normalize(): array
    {
        return [
            'columns' => $this->columns,
            'name' => $this->name,
        ];
    }
}
