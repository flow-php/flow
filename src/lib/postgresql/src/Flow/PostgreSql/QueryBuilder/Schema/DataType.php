<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema;

use Flow\PostgreSql\Protobuf\AST\{A_Const, Integer, Node, PBString, TypeName};

final readonly class DataType
{
    /**
     * @param list<string> $names
     * @param list<int> $typmods
     * @param list<int> $arrayBounds
     */
    private function __construct(
        private array $names,
        private array $typmods = [],
        private array $arrayBounds = [],
    ) {
    }

    public static function array(self $elementType) : self
    {
        return new self(
            $elementType->names,
            $elementType->typmods,
            [-1],
        );
    }

    public static function bigint() : self
    {
        return new self(['pg_catalog', 'int8']);
    }

    public static function bigserial() : self
    {
        return new self(['bigserial']);
    }

    public static function boolean() : self
    {
        return new self(['pg_catalog', 'bool']);
    }

    public static function bytea() : self
    {
        return new self(['pg_catalog', 'bytea']);
    }

    public static function char(int $length) : self
    {
        return new self(['pg_catalog', 'bpchar'], [$length]);
    }

    public static function cidr() : self
    {
        return new self(['pg_catalog', 'cidr']);
    }

    public static function custom(string $typeName, ?string $schema = null) : self
    {
        if ($schema !== null) {
            return new self([$schema, $typeName]);
        }

        return new self([$typeName]);
    }

    public static function date() : self
    {
        return new self(['pg_catalog', 'date']);
    }

    public static function decimal(?int $precision = null, ?int $scale = null) : self
    {
        return self::numeric($precision, $scale);
    }

    public static function doublePrecision() : self
    {
        return new self(['pg_catalog', 'float8']);
    }

    public static function inet() : self
    {
        return new self(['pg_catalog', 'inet']);
    }

    public static function integer() : self
    {
        return new self(['pg_catalog', 'int4']);
    }

    public static function interval() : self
    {
        return new self(['pg_catalog', 'interval']);
    }

    public static function json() : self
    {
        return new self(['pg_catalog', 'json']);
    }

    public static function jsonb() : self
    {
        return new self(['pg_catalog', 'jsonb']);
    }

    public static function macaddr() : self
    {
        return new self(['pg_catalog', 'macaddr']);
    }

    public static function numeric(?int $precision = null, ?int $scale = null) : self
    {
        $typmods = [];

        if ($precision !== null) {
            $typmods[] = $precision;

            if ($scale !== null) {
                $typmods[] = $scale;
            }
        }

        return new self(['pg_catalog', 'numeric'], $typmods);
    }

    public static function real() : self
    {
        return new self(['pg_catalog', 'float4']);
    }

    public static function serial() : self
    {
        return new self(['serial']);
    }

    public static function smallint() : self
    {
        return new self(['pg_catalog', 'int2']);
    }

    public static function smallserial() : self
    {
        return new self(['smallserial']);
    }

    public static function text() : self
    {
        return new self(['pg_catalog', 'text']);
    }

    public static function time(?int $precision = null) : self
    {
        $typmods = [];

        if ($precision !== null) {
            $typmods[] = $precision;
        }

        return new self(['pg_catalog', 'time'], $typmods);
    }

    public static function timestamp(?int $precision = null) : self
    {
        $typmods = [];

        if ($precision !== null) {
            $typmods[] = $precision;
        }

        return new self(['pg_catalog', 'timestamp'], $typmods);
    }

    public static function timestamptz(?int $precision = null) : self
    {
        $typmods = [];

        if ($precision !== null) {
            $typmods[] = $precision;
        }

        return new self(['pg_catalog', 'timestamptz'], $typmods);
    }

    public static function uuid() : self
    {
        return new self(['pg_catalog', 'uuid']);
    }

    public static function varchar(int $length) : self
    {
        return new self(['pg_catalog', 'varchar'], [$length]);
    }

    public function toAst() : TypeName
    {
        $typeName = new TypeName();

        $names = [];

        foreach ($this->names as $name) {
            $names[] = $this->createStringNode($name);
        }

        $typeName->setNames($names);

        if ($this->typmods !== []) {
            $typmods = [];

            foreach ($this->typmods as $typmod) {
                $typmods[] = $this->createIntegerConstNode($typmod);
            }

            $typeName->setTypmods($typmods);
        }

        if ($this->arrayBounds !== []) {
            $arrayBounds = [];

            foreach ($this->arrayBounds as $bound) {
                $arrayBounds[] = $this->createIntegerConstNode($bound);
            }

            $typeName->setArrayBounds($arrayBounds);
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
