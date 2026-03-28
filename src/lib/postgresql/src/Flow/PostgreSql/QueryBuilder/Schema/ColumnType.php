<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema;

use Flow\PostgreSql\Protobuf\AST\{A_Const, Integer, Node, PBString, TypeName};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class ColumnType
{
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private ?int $precision = null,
        private ?int $scale = null,
        private bool $isArray = false,
    ) {
    }

    public static function array(self $elementType) : self
    {
        return new self(
            $elementType->name,
            $elementType->schema,
            $elementType->precision,
            $elementType->scale,
            true,
        );
    }

    public static function bigint() : self
    {
        return new self('int8', 'pg_catalog');
    }

    public static function bigserial() : self
    {
        return new self('bigserial');
    }

    public static function boolean() : self
    {
        return new self('bool', 'pg_catalog');
    }

    public static function bytea() : self
    {
        return new self('bytea', 'pg_catalog');
    }

    public static function char(int $length) : self
    {
        return new self('bpchar', 'pg_catalog', $length);
    }

    public static function cidr() : self
    {
        return new self('cidr', 'pg_catalog');
    }

    public static function custom(string $typeName, ?string $schema = null) : self
    {
        return new self($typeName, $schema);
    }

    public static function date() : self
    {
        return new self('date', 'pg_catalog');
    }

    public static function decimal(?int $precision = null, ?int $scale = null) : self
    {
        return self::numeric($precision, $scale);
    }

    public static function doublePrecision() : self
    {
        return new self('float8', 'pg_catalog');
    }

    public static function fromAst(TypeName $typeName) : self
    {
        $namesNodes = $typeName->getNames();

        if ($namesNodes === null || \count($namesNodes) === 0) {
            throw InvalidAstException::missingRequiredField('names', 'TypeName');
        }

        $names = [];

        foreach ($namesNodes as $nameNode) {
            $stringNode = $nameNode->getString();

            if ($stringNode === null) {
                throw InvalidAstException::invalidFieldValue('names', 'TypeName', 'expected String node');
            }

            $names[] = $stringNode->getSval();
        }

        $schema = \count($names) > 1 ? $names[0] : null;
        $name = $names[\count($names) - 1];

        $typmods = [];
        $typmodsNodes = $typeName->getTypmods();

        foreach ($typmodsNodes as $typmodNode) {
            $aConst = $typmodNode->getAConst();

            if ($aConst !== null) {
                $ival = $aConst->getIval();

                if ($ival !== null) {
                    /** @phpstan-ignore method.nonObject (protobuf PHPDoc says int but getIval() actually returns Integer object) */
                    $typmods[] = $ival->getIval();
                }
            }
        }

        $isArray = false;
        $arrayBoundsNodes = $typeName->getArrayBounds();

        foreach ($arrayBoundsNodes as $boundNode) {
            $aConst = $boundNode->getAConst();

            if ($aConst !== null) {
                $ival = $aConst->getIval();

                if ($ival !== null) {
                    $isArray = true;

                    continue;
                }
            }

            $integer = $boundNode->getInteger();

            if ($integer !== null) {
                $isArray = true;
            }
        }

        return new self(
            $name,
            $schema,
            $typmods[0] ?? null,
            $typmods[1] ?? null,
            $isArray,
        );
    }

    public static function inet() : self
    {
        return new self('inet', 'pg_catalog');
    }

    public static function integer() : self
    {
        return new self('int4', 'pg_catalog');
    }

    public static function interval() : self
    {
        return new self('interval', 'pg_catalog');
    }

    public static function json() : self
    {
        return new self('json', 'pg_catalog');
    }

    public static function jsonb() : self
    {
        return new self('jsonb', 'pg_catalog');
    }

    public static function macaddr() : self
    {
        return new self('macaddr', 'pg_catalog');
    }

    public static function numeric(?int $precision = null, ?int $scale = null) : self
    {
        return new self('numeric', 'pg_catalog', $precision, $scale);
    }

    public static function real() : self
    {
        return new self('float4', 'pg_catalog');
    }

    public static function serial() : self
    {
        return new self('serial');
    }

    public static function smallint() : self
    {
        return new self('int2', 'pg_catalog');
    }

    public static function smallserial() : self
    {
        return new self('smallserial');
    }

    public static function text() : self
    {
        return new self('text', 'pg_catalog');
    }

    public static function time(?int $precision = null) : self
    {
        return new self('time', 'pg_catalog', $precision);
    }

    public static function timestamp(?int $precision = null) : self
    {
        return new self('timestamp', 'pg_catalog', $precision);
    }

    public static function timestamptz(?int $precision = null) : self
    {
        return new self('timestamptz', 'pg_catalog', $precision);
    }

    public static function uuid() : self
    {
        return new self('uuid', 'pg_catalog');
    }

    public static function varchar(int $length) : self
    {
        return new self('varchar', 'pg_catalog', $length);
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name
            && $this->schema === $other->schema
            && $this->precision === $other->precision
            && $this->scale === $other->scale
            && $this->isArray === $other->isArray;
    }

    public function toAst() : TypeName
    {
        $typeName = new TypeName();

        $names = [];

        if ($this->schema !== null) {
            $names[] = $this->createStringNode($this->schema);
        }

        $names[] = $this->createStringNode($this->name);
        $typeName->setNames($names);

        $typmods = [];

        if ($this->precision !== null) {
            $typmods[] = $this->createIntegerConstNode($this->precision);

            if ($this->scale !== null) {
                $typmods[] = $this->createIntegerConstNode($this->scale);
            }
        }

        if ($typmods !== []) {
            $typeName->setTypmods($typmods);
        }

        if ($this->isArray) {
            $typeName->setArrayBounds([$this->createIntegerConstNode(-1)]);
        }

        return $typeName;
    }

    private function createIntegerConstNode(int $value) : Node
    {
        $aConst = new A_Const();
        $ival = new Integer();
        $ival->setIval($value);
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $aConst->setIval($ival);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function createStringNode(string $value) : Node
    {
        $str = new PBString();
        $str->setSval($value);

        $node = new Node();
        $node->setString($str);

        return $node;
    }
}
