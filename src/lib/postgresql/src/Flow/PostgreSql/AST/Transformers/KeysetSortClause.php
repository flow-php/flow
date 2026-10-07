<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\Nodes\Column;
use Flow\PostgreSql\Exception\PaginationException;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SortByDir;
use Flow\PostgreSql\Protobuf\AST\SortByNulls;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

use function array_map;
use function count;
use function implode;
use function iterator_to_array;
use function sprintf;

final readonly class KeysetSortClause
{
    /**
     * @param list<KeysetColumn> $columns
     */
    public function __construct(
        private array $columns,
    ) {}

    /**
     * @param iterable<Node> $sortClause
     *
     * @throws PaginationException
     */
    public function assertMatches(iterable $sortClause): void
    {
        $items = iterator_to_array($sortClause, false);

        if (count($items) !== count($this->columns)) {
            throw new PaginationException(sprintf(
                'Keyset pagination requires ORDER BY to list exactly the keys %s, but the query orders by %d item(s)',
                $this->describe(),
                count($items),
            ));
        }

        foreach ($items as $index => $node) {
            $key = $this->columns[$index];
            $sortBy = $node->getSortBy();
            $columnRef = $sortBy?->getNode()?->getColumnRef();

            $direction = match ($sortBy?->getSortbyDir()) {
                SortByDir::SORTBY_DEFAULT, SortByDir::SORTBY_ASC => SortOrder::ASC,
                SortByDir::SORTBY_DESC => SortOrder::DESC,
                default => null,
            };

            if (
                $sortBy === null
                || $sortBy->getSortbyNulls() !== SortByNulls::SORTBY_NULLS_DEFAULT
                || $direction !== $key->order
                || $columnRef === null
                || (new Column($columnRef))->name() !== QualifiedIdentifier::parse($key->column)->name()
            ) {
                throw new PaginationException(sprintf(
                    'Keyset pagination requires ORDER BY item #%d to be the key "%s %s" with no NULLS clause, ordinal, expression or USING',
                    $index + 1,
                    $key->column,
                    $key->order->value,
                ));
            }
        }
    }

    public function describe(): string
    {
        return (
            '"'
            . implode(', ', array_map(
                static fn(KeysetColumn $column): string => $column->column . ' ' . $column->order->value,
                $this->columns,
            ))
            . '"'
        );
    }
}
