<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

use Flow\PostgreSql\Protobuf\AST\A_Star;
use Flow\PostgreSql\Protobuf\AST\CopyStmt;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

/**
 * Builder for COPY TO statements (data export).
 */
final readonly class CopyToBuilder implements CopyToDestinationStep, CopyToOptionsStep, CopyToTableStep
{
    use AstToSql;

    /**
     * @param list<string> $columns
     * @param list<string> $forceQuoteColumns
     */
    private function __construct(
        private ?string $table = null,
        private ?string $schema = null,
        private array $columns = [],
        private ?SelectFinalStep $query = null,
        private ?string $filename = null,
        private bool $isProgram = false,
        private bool $isStdout = false,
        private ?CopyFormat $format = null,
        private ?string $delimiter = null,
        private ?string $nullString = null,
        private ?bool $header = null,
        private ?string $quote = null,
        private ?string $escape = null,
        private array $forceQuoteColumns = [],
        private bool $forceQuoteAll = false,
        private ?string $encoding = null,
    ) {}

    public static function create(): CopyToTableStep
    {
        return new self();
    }

    public function columns(string ...$columns): CopyToDestinationStep
    {
        return new self(
            $this->table,
            $this->schema,
            \array_values($columns),
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function delimiter(string $delimiter): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function encoding(string $encoding): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $encoding,
        );
    }

    public function escape(string $escape): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function file(string $filename): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $filename,
            false,
            false,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function forceQuote(string ...$columns): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            \array_values($columns),
            false,
            $this->encoding,
        );
    }

    public function forceQuoteAll(): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            [],
            true,
            $this->encoding,
        );
    }

    public function format(CopyFormat $format): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function nullAs(string $nullString): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function program(string $command): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $command,
            true,
            false,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function query(SelectFinalStep $query): CopyToDestinationStep
    {
        return new self(
            null,
            null,
            [],
            $query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function quote(string $quote): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function stdout(): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            null,
            false,
            true,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function table(string $table, string ...$columns): CopyToDestinationStep
    {
        $identifier = QualifiedIdentifier::parse($table);

        return new self(
            $identifier->name(),
            $identifier->schema(),
            \array_values($columns),
            null,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    public function toAst(): CopyStmt
    {
        if ($this->table === null && $this->query === null) {
            throw InvalidExpressionException::invalidValue('table or query', 'null');
        }

        if (!$this->isStdout && ($this->filename === null || $this->filename === '')) {
            throw InvalidExpressionException::invalidValue('filename', 'null or empty');
        }

        $copyStmt = new CopyStmt();
        $copyStmt->setIsFrom(false);
        $copyStmt->setIsProgram($this->isProgram);

        if ($this->isStdout) {
            $copyStmt->setFilename('');
        } else {
            $copyStmt->setFilename(type_string()->assert($this->filename));
        }

        if ($this->table !== null) {
            $rangeVar = new RangeVar([
                'relname' => $this->table,
                'inh' => true,
            ]);

            if ($this->schema !== null) {
                $rangeVar->setSchemaname($this->schema);
            }

            $copyStmt->setRelation($rangeVar);

            if ($this->columns !== []) {
                $attlist = [];

                foreach ($this->columns as $column) {
                    $str = new PBString();
                    $str->setSval($column);
                    $node = new Node();
                    $node->setString($str);
                    $attlist[] = $node;
                }

                $copyStmt->setAttlist($attlist);
            }
        } else {
            $query = type_instance_of(SelectFinalStep::class)->assert($this->query);
            $queryNode = new Node();
            $queryNode->setSelectStmt($query->toAst());
            $copyStmt->setQuery($queryNode);
        }

        $options = $this->buildOptions();

        if ($options !== []) {
            $copyStmt->setOptions($options);
        }

        return $copyStmt;
    }

    public function withHeader(bool $header = true): CopyToOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->query,
            $this->filename,
            $this->isProgram,
            $this->isStdout,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $header,
            $this->quote,
            $this->escape,
            $this->forceQuoteColumns,
            $this->forceQuoteAll,
            $this->encoding,
        );
    }

    /**
     * @return list<Node>
     */
    private function buildOptions(): array
    {
        $options = [];

        if ($this->format !== null) {
            $options[] = $this->createStringOption('format', $this->format->value);
        }

        if ($this->delimiter !== null) {
            $options[] = $this->createStringOption('delimiter', $this->delimiter);
        }

        if ($this->nullString !== null) {
            $options[] = $this->createStringOption('null', $this->nullString);
        }

        if ($this->header !== null) {
            $options[] = $this->createBoolOption('header', $this->header);
        }

        if ($this->quote !== null) {
            $options[] = $this->createStringOption('quote', $this->quote);
        }

        if ($this->escape !== null) {
            $options[] = $this->createStringOption('escape', $this->escape);
        }

        if ($this->forceQuoteAll) {
            $options[] = $this->createStarOption('force_quote');
        } elseif ($this->forceQuoteColumns !== []) {
            $options[] = $this->createColumnListOption('force_quote', $this->forceQuoteColumns);
        }

        if ($this->encoding !== null) {
            $options[] = $this->createStringOption('encoding', $this->encoding);
        }

        return $options;
    }

    private function createBoolOption(string $name, bool $value): Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $str = new PBString();
        $str->setSval($value ? 'true' : 'false');
        $argNode = new Node();
        $argNode->setString($str);
        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }

    /**
     * @param list<string> $columns
     */
    private function createColumnListOption(string $name, array $columns): Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $columnNodes = [];

        foreach ($columns as $column) {
            $str = new PBString();
            $str->setSval($column);
            $colNode = new Node();
            $colNode->setString($str);
            $columnNodes[] = $colNode;
        }

        $listNode = new Node();
        $listNode->setList(new PBList(['items' => $columnNodes]));
        $defElem->setArg($listNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }

    private function createStarOption(string $name): Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $star = new A_Star();
        $starNode = new Node();
        $starNode->setAStar($star);
        $defElem->setArg($starNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }

    private function createStringOption(string $name, string $value): Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $str = new PBString();
        $str->setSval($value);
        $argNode = new Node();
        $argNode->setString($str);
        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }
}
