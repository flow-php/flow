<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser\ExpressionParser;

/**
 * @type IndexShape = array{name: string, columns: non-empty-list<string>, unique: bool, method: string, primary: bool, predicate: ?string}
 */
final readonly class Index
{
    public ?string $predicate;

    /**
     * @param non-empty-list<string> $columns
     */
    public function __construct(
        public string $name,
        public array $columns,
        public bool $unique = false,
        public IndexMethod $method = IndexMethod::BTREE,
        public bool $primary = false,
        ?string $predicate = null,
    ) {
        $this->predicate = $predicate !== null ? (new ExpressionParser())->normalize($predicate) : null;
    }

    /**
     * @param IndexShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            columns: $data['columns'],
            unique: $data['unique'],
            method: array_key_exists('method', $data) ? IndexMethod::from($data['method']) : IndexMethod::BTREE,
            primary: $data['primary'],
            predicate: $data['predicate'] ?? null,
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
            && $this->unique === $other->unique
            && $this->method === $other->method
            && $this->primary === $other->primary
            && $this->predicate === $other->predicate
        );
    }

    /**
     * @return IndexShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'columns' => $this->columns,
            'unique' => $this->unique,
            'method' => $this->method->value,
            'primary' => $this->primary,
            'predicate' => $this->predicate,
        ];
    }
}
