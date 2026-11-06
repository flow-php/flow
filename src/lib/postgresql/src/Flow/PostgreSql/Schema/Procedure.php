<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Schema\Function\FunctionArgument;
use Flow\PostgreSql\QueryBuilder\Sql;

use function array_map;
use function Flow\PostgreSql\DSL\column_type_from_string;
use function Flow\PostgreSql\DSL\create;

/**
 * @type ProcedureShape = array{name: string, argument_types: list<string>, language: string, definition: ?string, schema?: ?string}
 */
final readonly class Procedure
{
    /**
     * @param list<string> $argumentTypes
     */
    public function __construct(
        public string $name,
        public array $argumentTypes = [],
        public string $language = 'sql',
        public ?string $definition = null,
        public ?string $schema = null,
    ) {}

    /**
     * @param ProcedureShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            argumentTypes: $data['argument_types'],
            language: $data['language'],
            definition: $data['definition'] ?? null,
            schema: $data['schema'] ?? null,
        );
    }

    /**
     * @return ProcedureShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'argument_types' => $this->argumentTypes,
            'language' => $this->language,
            'definition' => $this->definition,
            'schema' => $this->schema,
        ];
    }

    public function qualifiedName(): string
    {
        return $this->schemaName() . '.' . $this->name;
    }

    public function schemaName(): string
    {
        return $this->schema ?? 'public';
    }

    public function toSql(): ?Sql
    {
        if ($this->definition === null) {
            return null;
        }

        $builder = create()->procedure($this->qualifiedName())->orReplace();

        if ($this->argumentTypes !== []) {
            $args = array_map(static fn(string $type): FunctionArgument => FunctionArgument::of(column_type_from_string(
                $type,
            )), $this->argumentTypes);
            $builder = $builder->arguments(...$args);
        }

        return $builder->language($this->language)->as($this->definition);
    }

    public function withSchema(string $schema): self
    {
        return new self($this->name, $this->argumentTypes, $this->language, $this->definition, $schema);
    }
}
