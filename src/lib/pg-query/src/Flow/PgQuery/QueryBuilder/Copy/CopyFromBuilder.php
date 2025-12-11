<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Copy;

use Flow\PgQuery\Protobuf\AST\{CopyStmt, DefElem, Node, PBList, PBString, RangeVar};
use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PgQuery\QueryBuilder\QualifiedIdentifier;

/**
 * Builder for COPY FROM statements (data import).
 */
final readonly class CopyFromBuilder implements CopyFromOptionsStep, CopyFromSourceStep, CopyFromTableStep
{
    /**
     * @param list<string> $columns
     * @param list<string> $forceNotNullColumns
     * @param list<string> $forceNullColumns
     */
    private function __construct(
        private ?string $table = null,
        private ?string $schema = null,
        private array $columns = [],
        private ?string $filename = null,
        private bool $isProgram = false,
        private bool $isStdin = false,
        private ?CopyFormat $format = null,
        private ?string $delimiter = null,
        private ?string $nullString = null,
        private ?bool $header = null,
        private ?string $quote = null,
        private ?string $escape = null,
        private array $forceNotNullColumns = [],
        private array $forceNullColumns = [],
        private ?string $encoding = null,
        private ?CopyOnError $onError = null,
    ) {
    }

    public static function create() : CopyFromTableStep
    {
        return new self();
    }

    public function delimiter(string $delimiter) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function encoding(string $encoding) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $encoding,
            $this->onError,
        );
    }

    public function escape(string $escape) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function forceNotNull(string ...$columns) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            \array_values($columns),
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function forceNull(string ...$columns) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            \array_values($columns),
            $this->encoding,
            $this->onError,
        );
    }

    public function format(CopyFormat $format) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function fromFile(string $filename) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $filename,
            false,
            false,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function fromProgram(string $command) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $command,
            true,
            false,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function fromStdin() : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            null,
            false,
            true,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function nullAs(string $nullString) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function onError(CopyOnError $behavior) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $behavior,
        );
    }

    public function quote(string $quote) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function table(string $table, string ...$columns) : CopyFromSourceStep
    {
        $identifier = QualifiedIdentifier::parse($table);

        return new self(
            $identifier->name(),
            $identifier->schema(),
            \array_values($columns),
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $this->header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    public function toAst() : CopyStmt
    {
        if ($this->table === null || $this->table === '') {
            throw InvalidExpressionException::invalidValue('table', 'null or empty');
        }

        if (!$this->isStdin && ($this->filename === null || $this->filename === '')) {
            throw InvalidExpressionException::invalidValue('filename', 'null or empty');
        }

        $copyStmt = new CopyStmt();
        $copyStmt->setIsFrom(true);
        $copyStmt->setIsProgram($this->isProgram);

        if ($this->isStdin) {
            $copyStmt->setFilename('');
        } else {
            $copyStmt->setFilename($this->filename ?? '');
        }

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

        $options = $this->buildOptions();

        if ($options !== []) {
            $copyStmt->setOptions($options);
        }

        return $copyStmt;
    }

    public function withHeader(bool $header = true) : CopyFromOptionsStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->columns,
            $this->filename,
            $this->isProgram,
            $this->isStdin,
            $this->format,
            $this->delimiter,
            $this->nullString,
            $header,
            $this->quote,
            $this->escape,
            $this->forceNotNullColumns,
            $this->forceNullColumns,
            $this->encoding,
            $this->onError,
        );
    }

    /**
     * @return list<Node>
     */
    private function buildOptions() : array
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

        if ($this->forceNotNullColumns !== []) {
            $options[] = $this->createColumnListOption('force_not_null', $this->forceNotNullColumns);
        }

        if ($this->forceNullColumns !== []) {
            $options[] = $this->createColumnListOption('force_null', $this->forceNullColumns);
        }

        if ($this->encoding !== null) {
            $options[] = $this->createStringOption('encoding', $this->encoding);
        }

        if ($this->onError !== null) {
            $options[] = $this->createStringOption('on_error', $this->onError->value);
        }

        return $options;
    }

    private function createBoolOption(string $name, bool $value) : Node
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
    private function createColumnListOption(string $name, array $columns) : Node
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

    private function createStringOption(string $name, string $value) : Node
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
