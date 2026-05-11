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
            $ival = $aConst->getIval();
            \assert($ival instanceof Integer);

            return new self($ival->getIval());
        }

        if ($aConst->hasFval()) {
            $fval = $aConst->getFval();
            \assert($fval instanceof PBFloat);
            $floatStr = $fval->getFval();

            return new self((float) $floatStr);
        }

        if ($aConst->hasSval()) {
            $sval = $aConst->getSval();
            \assert($sval instanceof PBString);

            return new self($sval->getSval());
        }

        if ($aConst->hasBoolval()) {
            $boolval = $aConst->getBoolval();
            \assert($boolval instanceof Boolean);

            return new self($boolval->getBoolval());
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
        $aConst = new A_Const();

        if ($this->value === null) {
            $aConst->setIsnull(true);
        } elseif (\is_int($this->value)) {
            $integer = new Integer();
            $integer->setIval($this->value);

            /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
            $aConst->setIval($integer);
        } elseif (\is_float($this->value)) {
            $float = new PBFloat();
            $float->setFval((string) $this->value);
            $aConst->setFval($float);
        } elseif (\is_string($this->value)) {
            $string = new PBString();
            $string->setSval($this->value);
            $aConst->setSval($string);
        } elseif (\is_bool($this->value)) {
            $boolean = new Boolean();
            $boolean->setBoolval($this->value);

            /** @phpstan-ignore argument.type (protobuf PHPDoc says bool but actually expects Boolean) */
            $aConst->setBoolval($boolean);
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
