<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Function\{ListFunctions, ScalarFunctionChain, StructureFunctions};

final class EntryReference extends ScalarFunctionChain implements Reference
{
    private ?string $alias = null;

    private SortOrder $sort = SortOrder::ASC;

    public function __construct(private readonly string $entry)
    {
    }

    public static function init(string|Reference $ref) : Reference
    {
        if (\is_string($ref)) {
            return new self($ref);
        }

        return $ref;
    }

    #[\Override]
    public function __toString() : string
    {
        return $this->name();
    }

    #[\Override]
    public function as(string $alias) : self
    {
        $this->alias = $alias;

        return $this;
    }

    public function asc() : self
    {
        $this->sort = SortOrder::ASC;

        return $this;
    }

    #[\Override]
    public function base() : string
    {
        return $this->entry;
    }

    public function desc() : self
    {
        $this->sort = SortOrder::DESC;

        return $this;
    }

    #[\Override]
    public function eval(Row $row, FlowContext $context) : mixed
    {
        return $row->valueOf($this->entry);
    }

    #[\Override]
    public function hasAlias() : bool
    {
        return $this->alias !== null;
    }

    #[\Override]
    public function is(Reference $ref) : bool
    {
        return $this->name() === $ref->name();
    }

    public function list() : ListFunctions
    {
        return new ListFunctions($this);
    }

    #[\Override]
    public function name() : string
    {
        return $this->alias ?? $this->entry;
    }

    #[\Override]
    public function sort() : SortOrder
    {
        return $this->sort;
    }

    public function structure() : StructureFunctions
    {
        return new StructureFunctions($this);
    }

    #[\Override]
    public function to() : string
    {
        return $this->entry;
    }
}
