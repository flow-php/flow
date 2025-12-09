<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\CreateSequence;

use Flow\PgQuery\Protobuf\AST\{Boolean, CreateSeqStmt, DefElem, Integer, Node, PBList, PBString, RangeVar, TypeName};

final readonly class CreateSequenceBuilder implements CreateSequenceNameStep, CreateSequenceOptionsStep
{
    /**
     * @param array<array{name: string, arg: null|Node}> $options
     */
    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private bool $ifNotExists = false,
        private bool $temporary = false,
        private bool $unlogged = false,
        private array $options = [],
    ) {
    }

    public static function create() : CreateSequenceNameStep
    {
        return new self();
    }

    public static function createIfNotExists() : CreateSequenceNameStep
    {
        return new self(ifNotExists: true);
    }

    public static function createTemporary() : CreateSequenceNameStep
    {
        return new self(temporary: true);
    }

    public static function createUnlogged() : CreateSequenceNameStep
    {
        return new self(unlogged: true);
    }

    public function asType(string $dataType) : CreateSequenceOptionsStep
    {
        $typeMap = [
            'smallint' => 'int2',
            'integer' => 'int4',
            'bigint' => 'int8',
            'int2' => 'int2',
            'int4' => 'int4',
            'int8' => 'int8',
        ];

        $pgType = $typeMap[\strtolower($dataType)] ?? $dataType;

        $typeName = new TypeName();
        $names = [];

        $catalogStr = new PBString();
        $catalogStr->setSval('pg_catalog');
        $catalogNode = new Node();
        $catalogNode->setString($catalogStr);
        $names[] = $catalogNode;

        $typeStr = new PBString();
        $typeStr->setSval($pgType);
        $typeNode = new Node();
        $typeNode->setString($typeStr);
        $names[] = $typeNode;

        $typeName->setNames($names);
        $typeName->setTypemod(-1);

        $argNode = new Node();
        $argNode->setTypeName($typeName);

        return $this->withOption('as', $argNode);
    }

    public function cache(int $cache) : CreateSequenceOptionsStep
    {
        return $this->withIntegerOption('cache', $cache);
    }

    public function cycle() : CreateSequenceOptionsStep
    {
        return $this->withBooleanOption('cycle', true);
    }

    public function ifNotExists() : self
    {
        return new self(
            $this->name,
            $this->schema,
            true,
            $this->temporary,
            $this->unlogged,
            $this->options,
        );
    }

    public function incrementBy(int $increment) : CreateSequenceOptionsStep
    {
        return $this->withIntegerOption('increment', $increment);
    }

    public function maxValue(int $maxValue) : CreateSequenceOptionsStep
    {
        return $this->withIntegerOption('maxvalue', $maxValue);
    }

    public function minValue(int $minValue) : CreateSequenceOptionsStep
    {
        return $this->withIntegerOption('minvalue', $minValue);
    }

    public function noCycle() : CreateSequenceOptionsStep
    {
        return $this->withBooleanOption('cycle', false);
    }

    public function noMaxValue() : CreateSequenceOptionsStep
    {
        return $this->withOption('maxvalue', null);
    }

    public function noMinValue() : CreateSequenceOptionsStep
    {
        return $this->withOption('minvalue', null);
    }

    public function ownedBy(string $table, string $column) : CreateSequenceOptionsStep
    {
        $list = new PBList();
        $items = [];

        $tableParts = \explode('.', $table);

        foreach ($tableParts as $part) {
            $str = new PBString();
            $str->setSval($part);
            $strNode = new Node();
            $strNode->setString($str);
            $items[] = $strNode;
        }

        $colStr = new PBString();
        $colStr->setSval($column);
        $colNode = new Node();
        $colNode->setString($colStr);
        $items[] = $colNode;

        $list->setItems($items);

        $argNode = new Node();
        $argNode->setList($list);

        return $this->withOption('owned_by', $argNode);
    }

    public function ownedByNone() : CreateSequenceOptionsStep
    {
        $list = new PBList();
        $items = [];

        $str = new PBString();
        $str->setSval('none');
        $strNode = new Node();
        $strNode->setString($str);
        $items[] = $strNode;

        $list->setItems($items);

        $argNode = new Node();
        $argNode->setList($list);

        return $this->withOption('owned_by', $argNode);
    }

    public function sequence(string $name, ?string $schema = null) : CreateSequenceOptionsStep
    {
        return new self(
            $name,
            $schema,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $this->options,
        );
    }

    public function startWith(int $start) : CreateSequenceOptionsStep
    {
        return $this->withIntegerOption('start', $start);
    }

    public function temporary() : self
    {
        return new self(
            $this->name,
            $this->schema,
            $this->ifNotExists,
            true,
            false,
            $this->options,
        );
    }

    public function toAst() : CreateSeqStmt
    {
        $stmt = new CreateSeqStmt();

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->name ?? '');
        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        if ($this->temporary) {
            $rangeVar->setRelpersistence('t');
        } elseif ($this->unlogged) {
            $rangeVar->setRelpersistence('u');
        } else {
            $rangeVar->setRelpersistence('p');
        }

        $stmt->setSequence($rangeVar);

        if ($this->ifNotExists) {
            $stmt->setIfNotExists(true);
        }

        if ($this->options !== []) {
            $optionNodes = [];

            foreach ($this->options as $option) {
                $defElem = new DefElem();
                $defElem->setDefname($option['name']);

                if ($option['arg'] !== null) {
                    $defElem->setArg($option['arg']);
                }

                $node = new Node();
                $node->setDefElem($defElem);
                $optionNodes[] = $node;
            }

            $stmt->setOptions($optionNodes);
        }

        return $stmt;
    }

    public function unlogged() : self
    {
        return new self(
            $this->name,
            $this->schema,
            $this->ifNotExists,
            false,
            true,
            $this->options,
        );
    }

    private function withBooleanOption(string $name, bool $value) : self
    {
        $boolean = new Boolean();
        $boolean->setBoolval($value);

        $argNode = new Node();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says bool but actually expects Boolean) */
        $argNode->setBoolean($boolean);

        return $this->withOption($name, $argNode);
    }

    private function withIntegerOption(string $name, int $value) : self
    {
        $integer = new Integer();
        $integer->setIval($value);

        $argNode = new Node();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $argNode->setInteger($integer);

        return $this->withOption($name, $argNode);
    }

    private function withOption(string $name, ?Node $arg) : self
    {
        $newOptions = $this->options;
        $newOptions[] = ['name' => $name, 'arg' => $arg];

        return new self(
            $this->name,
            $this->schema,
            $this->ifNotExists,
            $this->temporary,
            $this->unlogged,
            $newOptions,
        );
    }
}
