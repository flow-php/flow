<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Double;

use ArrayIterator;
use OpenSpout\Common\Entity\Cell;
use OpenSpout\Common\Entity\Row;
use OpenSpout\Reader\RowIteratorInterface;
use OpenSpout\Reader\SheetInterface;

use function array_map;

/**
 * @implements SheetInterface<RowIteratorInterface>
 */
final class FakeSheet implements SheetInterface
{
    /**
     * @var list<Row>
     */
    private readonly array $rows;

    /**
     * @param list<list<null|bool|float|int|string>> $rows
     */
    public function __construct(
        array $rows,
        private readonly string $name = 'Sheet1',
    ) {
        $this->rows = array_map(
            static fn(array $cells): Row => new Row(array_map(
                static fn(bool|float|int|string|null $cell): Cell => Cell::fromValue($cell),
                $cells,
            )),
            $rows,
        );
    }

    public function getIndex(): int
    {
        return 0;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getRowIterator(): RowIteratorInterface
    {
        return new FakeRowIterator(new ArrayIterator($this->rows));
    }

    public function isActive(): bool
    {
        return true;
    }
}
