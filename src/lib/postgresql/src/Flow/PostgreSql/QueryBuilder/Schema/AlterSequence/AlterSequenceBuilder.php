<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterSeqStmt;
use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\TypeName;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

use function strtolower;

final readonly class AlterSequenceBuilder implements AlterSequenceNameStep, AlterSequenceOptionsStep
{
    use AstToSql;

    /**
     * @param array<array{name: string, arg: null|Node}> $options
     */
    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private bool $ifExists = false,
        private array $options = [],
    ) {}

    public static function create(): AlterSequenceNameStep
    {
        return new self();
    }

    public static function ifExists(): AlterSequenceNameStep
    {
        return new self(ifExists: true);
    }

    public function asType(string $dataType): AlterSequenceOptionsStep
    {
        $typeMap = [
            'smallint' => 'int2',
            'integer' => 'int4',
            'bigint' => 'int8',
            'int2' => 'int2',
            'int4' => 'int4',
            'int8' => 'int8',
        ];

        $pgType = $typeMap[strtolower($dataType)] ?? $dataType;

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

    public function cache(int $cache): AlterSequenceOptionsStep
    {
        return $this->withIntegerOption('cache', $cache);
    }

    public function cycle(): AlterSequenceOptionsStep
    {
        return $this->withBooleanOption('cycle', true);
    }

    public function incrementBy(int $increment): AlterSequenceOptionsStep
    {
        return $this->withIntegerOption('increment', $increment);
    }

    public function maxValue(int $maxValue): AlterSequenceOptionsStep
    {
        return $this->withIntegerOption('maxvalue', $maxValue);
    }

    public function minValue(int $minValue): AlterSequenceOptionsStep
    {
        return $this->withIntegerOption('minvalue', $minValue);
    }

    public function noCycle(): AlterSequenceOptionsStep
    {
        return $this->withBooleanOption('cycle', false);
    }

    public function noMaxValue(): AlterSequenceOptionsStep
    {
        return $this->withOption('maxvalue', null);
    }

    public function noMinValue(): AlterSequenceOptionsStep
    {
        return $this->withOption('minvalue', null);
    }

    public function ownedBy(string $table, string $column): AlterSequenceOptionsStep
    {
        $list = new PBList();
        $items = [];

        $identifier = QualifiedIdentifier::parse($table);

        foreach ($identifier->parts() as $part) {
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

    public function ownedByNone(): AlterSequenceOptionsStep
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

    public function ownerTo(string $owner): AlterSequenceOwnerFinalStep
    {
        return AlterSequenceOwnerBuilder::create($this->name ?? '', $this->schema, $owner, $this->ifExists);
    }

    public function renameTo(string $newName): RenameSequenceFinalStep
    {
        return RenameSequenceBuilder::create($this->name ?? '', $this->schema, $newName, $this->ifExists);
    }

    public function restart(): AlterSequenceOptionsStep
    {
        return $this->withOption('restart', null);
    }

    public function restartWith(int $restart): AlterSequenceOptionsStep
    {
        return $this->withIntegerOption('restart', $restart);
    }

    public function sequence(string $name, ?string $schema = null): AlterSequenceOptionsStep
    {
        return new self($name, $schema, $this->ifExists, $this->options);
    }

    public function setLogged(): AlterSequenceLoggingFinalStep
    {
        return AlterSequenceLoggingBuilder::createLogged($this->name ?? '', $this->schema, $this->ifExists);
    }

    public function setSchema(string $schema): AlterSequenceSchemaFinalStep
    {
        return AlterSequenceSchemaBuilder::create($this->name ?? '', $this->schema, $schema, $this->ifExists);
    }

    public function setUnlogged(): AlterSequenceLoggingFinalStep
    {
        return AlterSequenceLoggingBuilder::createUnlogged($this->name ?? '', $this->schema, $this->ifExists);
    }

    public function startWith(int $start): AlterSequenceOptionsStep
    {
        return $this->withIntegerOption('start', $start);
    }

    public function toAst(): AlterSeqStmt
    {
        $stmt = new AlterSeqStmt();

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->name ?? '');
        $rangeVar->setInh(true);
        $rangeVar->setRelpersistence('p');

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setSequence($rangeVar);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
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

    public function withIfExists(): self
    {
        return new self($this->name, $this->schema, true, $this->options);
    }

    private function withBooleanOption(string $name, bool $value): self
    {
        $boolean = new Boolean();
        $boolean->setBoolval($value);

        $argNode = new Node(['boolean' => $boolean]);

        return $this->withOption($name, $argNode);
    }

    private function withIntegerOption(string $name, int $value): self
    {
        $integer = new Integer();
        $integer->setIval($value);

        $argNode = new Node(['integer' => $integer]);

        return $this->withOption($name, $argNode);
    }

    private function withOption(string $name, ?Node $arg): self
    {
        $newOptions = $this->options;
        $newOptions[] = ['name' => $name, 'arg' => $arg];

        return new self($this->name, $this->schema, $this->ifExists, $newOptions);
    }
}
