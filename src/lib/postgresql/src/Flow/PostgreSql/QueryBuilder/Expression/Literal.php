<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBFloat;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

use function Flow\Types\DSL\type_instance_of;

/**
 * Represents a literal value in SQL (string, int, float, bool, null).
 */
final readonly class Literal implements Expression
{
    private function __construct(
        private string|int|float|bool|null $value,
    ) {}

    public static function bool(bool $value): self
    {
        return new self($value);
    }

    public static function float(float $value): self
    {
        return new self($value);
    }

    public static function fromAst(Node $node): static
    {
        $aConst = $node->getAConst();

        if ($aConst === null) {
            throw InvalidAstException::unexpectedNodeType('A_Const', 'unknown');
        }

        if ($aConst->getIsnull()) {
            return new self(null);
        }

        if ($aConst->hasIval()) {
            return new self(type_instance_of(Integer::class)->assert($aConst->getIval())->getIval());
        }

        if ($aConst->hasFval()) {
            $fval = type_instance_of(PBFloat::class)->assert($aConst->getFval());

            return new self(\floatval($fval->getFval()));
        }

        if ($aConst->hasSval()) {
            $sval = $aConst->getSval();
            \assert($sval instanceof PBString);

            return new self($sval->getSval());
        }

        if ($aConst->hasBoolval()) {
            return new self(type_instance_of(Boolean::class)->assert($aConst->getBoolval())->getBoolval());
        }

        throw InvalidAstException::missingRequiredField('value', 'A_Const');
    }

    public static function int(int $value): self
    {
        return new self($value);
    }

    public static function null(): self
    {
        return new self(null);
    }

    public static function string(string $value): self
    {
        return new self($value);
    }

    public function as(string $alias): AliasedExpression
    {
        return AliasedExpression::create($this, $alias);
    }

    public function isBool(): bool
    {
        return \is_bool($this->value);
    }

    public function isFloat(): bool
    {
        return \is_float($this->value);
    }

    public function isInt(): bool
    {
        return \is_int($this->value);
    }

    public function isNull(): bool
    {
        return $this->value === null;
    }

    public function isString(): bool
    {
        return \is_string($this->value);
    }

    public function toAst(): Node
    {
        if ($this->value === null) {
            $aConst = new A_Const();
            $aConst->setIsnull(true);
        } elseif (\is_int($this->value)) {
            $integer = new Integer();
            $integer->setIval($this->value);
            $aConst = new A_Const(['ival' => $integer]);
        } elseif (\is_float($this->value)) {
            $aConst = new A_Const();
            $float = new PBFloat();
            $float->setFval((string) $this->value);
            $aConst->setFval($float);
        } elseif (\is_string($this->value)) {
            $aConst = new A_Const();
            $string = new PBString();
            $string->setSval($this->value);
            $aConst->setSval($string);
        } else {
            $boolean = new Boolean();
            $boolean->setBoolval($this->value);
            $aConst = new A_Const(['boolval' => $boolean]);
        }

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    public function value(): string|int|float|bool|null
    {
        return $this->value;
    }
}
