<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index;

use Flow\PostgreSql\Protobuf\AST\{IndexElem, Node, PBString, SortByDir, SortByNulls};
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

final readonly class IndexColumn
{
    private function __construct(
        private ?string $name,
        private ?Expression $expression,
        private ?string $opclass,
        private int $ordering,
        private int $nullsOrdering,
        private ?string $collation,
    ) {
    }

    public static function column(string $name) : self
    {
        return new self(
            $name,
            null,
            null,
            SortByDir::SORT_BY_DIR_UNDEFINED,
            SortByNulls::SORT_BY_NULLS_UNDEFINED,
            null,
        );
    }

    public static function expression(Expression $expression) : self
    {
        return new self(
            null,
            $expression,
            null,
            SortByDir::SORT_BY_DIR_UNDEFINED,
            SortByNulls::SORT_BY_NULLS_UNDEFINED,
            null,
        );
    }

    public function asc() : self
    {
        return new self(
            $this->name,
            $this->expression,
            $this->opclass,
            SortByDir::SORTBY_ASC,
            $this->nullsOrdering,
            $this->collation,
        );
    }

    public function collate(string $collation) : self
    {
        return new self(
            $this->name,
            $this->expression,
            $this->opclass,
            $this->ordering,
            $this->nullsOrdering,
            $collation,
        );
    }

    public function desc() : self
    {
        return new self(
            $this->name,
            $this->expression,
            $this->opclass,
            SortByDir::SORTBY_DESC,
            $this->nullsOrdering,
            $this->collation,
        );
    }

    public function nullsFirst() : self
    {
        return new self(
            $this->name,
            $this->expression,
            $this->opclass,
            $this->ordering,
            SortByNulls::SORTBY_NULLS_FIRST,
            $this->collation,
        );
    }

    public function nullsLast() : self
    {
        return new self(
            $this->name,
            $this->expression,
            $this->opclass,
            $this->ordering,
            SortByNulls::SORTBY_NULLS_LAST,
            $this->collation,
        );
    }

    public function opclass(string $opclass) : self
    {
        return new self(
            $this->name,
            $this->expression,
            $opclass,
            $this->ordering,
            $this->nullsOrdering,
            $this->collation,
        );
    }

    public function toAst() : IndexElem
    {
        $elem = new IndexElem();

        if ($this->name !== null) {
            $elem->setName($this->name);
        }

        if ($this->expression !== null) {
            $elem->setExpr($this->expression->toAst());
        }

        if ($this->ordering !== SortByDir::SORT_BY_DIR_UNDEFINED) {
            $elem->setOrdering($this->ordering);
        }

        if ($this->nullsOrdering !== SortByNulls::SORT_BY_NULLS_UNDEFINED) {
            $elem->setNullsOrdering($this->nullsOrdering);
        }

        if ($this->opclass !== null) {
            $opclassString = new PBString();
            $opclassString->setSval($this->opclass);

            $opclassNode = new Node();
            $opclassNode->setString($opclassString);

            $elem->setOpclass([$opclassNode]);
        }

        if ($this->collation !== null) {
            $collationString = new PBString();
            $collationString->setSval($this->collation);

            $collationNode = new Node();
            $collationNode->setString($collationString);

            $elem->setCollation([$collationNode]);
        }

        return $elem;
    }
}
