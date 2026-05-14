<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateView;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\ViewCheckOption;
use Flow\PostgreSql\Protobuf\AST\ViewStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

final readonly class CreateViewBuilder implements
    CreateViewAsStep,
    CreateViewCheckOptionStep,
    CreateViewFinalStep,
    CreateViewOptionsStep
{
    use AstToSql;

    /**
     * @param list<string> $columns
     */
    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private array $columns = [],
        private ?SelectFinalStep $query = null,
        private bool $replace = false,
        private bool $temporary = false,
        private bool $recursive = false,
        private ?int $checkOption = null,
    ) {}

    public static function create(string $name, ?string $schema = null): CreateViewOptionsStep
    {
        if ($schema !== null) {
            return new self($name, $schema);
        }

        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function as(SelectFinalStep $query): CreateViewCheckOptionStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $query,
            $this->replace,
            $this->temporary,
            $this->recursive,
            $this->checkOption,
        );
    }

    public function columns(string ...$columns): CreateViewAsStep
    {
        return new self(
            $this->name,
            $this->schema,
            \array_values($columns),
            $this->query,
            $this->replace,
            $this->temporary,
            $this->recursive,
            $this->checkOption,
        );
    }

    public function orReplace(): CreateViewOptionsStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            true,
            $this->temporary,
            $this->recursive,
            $this->checkOption,
        );
    }

    public function recursive(): CreateViewOptionsStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->replace,
            $this->temporary,
            true,
            $this->checkOption,
        );
    }

    public function temporary(): CreateViewOptionsStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->replace,
            true,
            $this->recursive,
            $this->checkOption,
        );
    }

    public function toAst(): ViewStmt
    {
        if ($this->name === null || $this->name === '') {
            throw InvalidExpressionException::invalidValue('view name', 'null or empty');
        }

        $query = $this->query;

        if ($query === null) {
            throw InvalidExpressionException::invalidValue('query', 'null');
        }

        $stmt = new ViewStmt();

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->name);

        if ($this->temporary) {
            $rangeVar->setRelpersistence('t');
        } else {
            $rangeVar->setRelpersistence('p');
        }

        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setView($rangeVar);

        $stmt->setReplace($this->replace);

        if ($this->columns !== []) {
            $aliases = [];

            foreach ($this->columns as $column) {
                $str = new PBString();
                $str->setSval($column);
                $node = new Node();
                $node->setString($str);
                $aliases[] = $node;
            }

            $stmt->setAliases($aliases);
        }

        $queryNode = new Node();
        $queryNode->setSelectStmt($query->toAst());
        $stmt->setQuery($queryNode);

        if ($this->checkOption !== null) {
            $stmt->setWithCheckOption($this->checkOption);
        }

        return $stmt;
    }

    public function withCascadedCheckOption(): CreateViewFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->replace,
            $this->temporary,
            $this->recursive,
            ViewCheckOption::CASCADED_CHECK_OPTION,
        );
    }

    public function withCheckOption(): CreateViewFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->replace,
            $this->temporary,
            $this->recursive,
            ViewCheckOption::CASCADED_CHECK_OPTION,
        );
    }

    public function withLocalCheckOption(): CreateViewFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->replace,
            $this->temporary,
            $this->recursive,
            ViewCheckOption::LOCAL_CHECK_OPTION,
        );
    }
}
