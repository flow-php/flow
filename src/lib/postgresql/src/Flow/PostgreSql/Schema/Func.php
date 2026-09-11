<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Schema\Function\FunctionArgument;
use Flow\PostgreSql\QueryBuilder\Sql;

use function array_map;
use function Flow\PostgreSql\DSL\column_type_from_string;
use function Flow\PostgreSql\DSL\create;

/**
 * @type FuncShape = array{name: string, return_type: string, argument_types: list<string>, language: string, definition: ?string, is_strict: bool, volatility: ?string}
 */
final readonly class Func
{
    /**
     * @param list<string> $argumentTypes
     */
    public function __construct(
        public string $name,
        public string $returnType,
        public array $argumentTypes = [],
        public string $language = 'sql',
        public ?string $definition = null,
        public bool $isStrict = false,
        public ?FunctionVolatility $volatility = null,
    ) {}

    /**
     * @param FuncShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            returnType: $data['return_type'],
            argumentTypes: $data['argument_types'],
            language: $data['language'],
            definition: $data['definition'] ?? null,
            isStrict: $data['is_strict'],
            volatility: array_key_exists('volatility', $data) && $data['volatility'] !== null
                ? FunctionVolatility::from($data['volatility'])
                : null,
        );
    }

    /**
     * @return FuncShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'return_type' => $this->returnType,
            'argument_types' => $this->argumentTypes,
            'language' => $this->language,
            'definition' => $this->definition,
            'is_strict' => $this->isStrict,
            'volatility' => $this->volatility?->value,
        ];
    }

    public function toSql(): ?Sql
    {
        if ($this->definition === null) {
            return null;
        }

        $builder = create()->function($this->name)->orReplace();

        if ($this->argumentTypes !== []) {
            $args = array_map(static fn(string $type): FunctionArgument => FunctionArgument::of(column_type_from_string(
                $type,
            )), $this->argumentTypes);
            $builder = $builder->arguments(...$args);
        }

        $builder = $builder->returns(column_type_from_string($this->returnType))->language($this->language);

        if ($this->isStrict) {
            $builder = $builder->strict();
        }

        if ($this->volatility !== null) {
            $builder = match ($this->volatility) {
                FunctionVolatility::IMMUTABLE => $builder->immutable(),
                FunctionVolatility::STABLE => $builder->stable(),
                FunctionVolatility::VOLATILE => $builder->volatile(),
            };
        }

        return $builder->as($this->definition);
    }
}
