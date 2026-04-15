<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

/**
 * @phpstan-import-type ColumnTypeShape from ColumnType
 *
 * @phpstan-type ColumnShape = array{name: string, type: ColumnTypeShape, nullable: bool, default: ?string, is_identity: bool, identity_generation: ?string, is_generated: bool, generation_expression: ?string, ordinal_position: ?int}
 */
final readonly class Column
{
    public ?string $generationExpression;

    public function __construct(
        public string $name,
        public ColumnType $type,
        public bool $nullable,
        public ?string $default = null,
        public bool $isIdentity = false,
        public ?IdentityGeneration $identityGeneration = null,
        public bool $isGenerated = false,
        ?string $generationExpression = null,
        public ?int $ordinalPosition = null,
    ) {
        $this->generationExpression = $generationExpression !== null
            ? (new ExpressionParser(new Parser()))->normalize($generationExpression)
            : null;
    }

    public static function create(
        string $name,
        ColumnType $type,
        bool $nullable = true,
        bool|float|int|string|Expression|null $default = null,
        bool $isIdentity = false,
        ?IdentityGeneration $identityGeneration = null,
        bool $isGenerated = false,
        ?string $generationExpression = null,
        ?int $ordinalPosition = null,
    ) : self {
        return new self(
            $name,
            $type,
            $nullable,
            (new ColumnDefaultFormatter())->format($default),
            $isIdentity,
            $identityGeneration,
            $isGenerated,
            $generationExpression,
            $ordinalPosition,
        );
    }

    /**
     * @param ColumnShape $data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            name: $data['name'],
            type: ColumnType::fromArray($data['type']),
            nullable: $data['nullable'],
            default: $data['default'] ?? null,
            isIdentity: $data['is_identity'] ?? false,
            identityGeneration: array_key_exists('identity_generation', $data) && $data['identity_generation'] !== null ? IdentityGeneration::from($data['identity_generation']) : null,
            isGenerated: $data['is_generated'] ?? false,
            generationExpression: $data['generation_expression'] ?? null,
            ordinalPosition: $data['ordinal_position'] ?? null,
        );
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other) : bool
    {
        return $this->type->isEqual($other->type)
            && $this->nullable === $other->nullable
            && $this->default === $other->default
            && $this->isIdentity === $other->isIdentity
            && $this->identityGeneration === $other->identityGeneration
            && $this->isGenerated === $other->isGenerated
            && $this->generationExpression === $other->generationExpression;
    }

    /**
     * @return ColumnShape
     */
    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'type' => $this->type->normalize(),
            'nullable' => $this->nullable,
            'default' => $this->default,
            'is_identity' => $this->isIdentity,
            'identity_generation' => $this->identityGeneration?->value,
            'is_generated' => $this->isGenerated,
            'generation_expression' => $this->generationExpression,
            'ordinal_position' => $this->ordinalPosition,
        ];
    }
}
