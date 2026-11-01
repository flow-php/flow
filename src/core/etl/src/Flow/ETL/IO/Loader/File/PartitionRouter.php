<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\IO\Loader\RowPartitions;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\Bucketing\KeyGrouping;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Partitions;
use Generator;

use function implode;
use function sprintf;

final class PartitionRouter
{
    private readonly RowPartitions $rowPartitions;

    /**
     * @var list<string> the partition columns the files do not carry
     */
    private readonly array $dropped;

    public function __construct(
        private readonly Partitioning $partitioning,
    ) {
        $this->rowPartitions = new RowPartitions($partitioning->by);
        $this->dropped = $partitioning->writeColumns ? [] : $partitioning->by->names();
    }

    /**
     * @return list<string>
     */
    public function pathOnlyColumns(): array
    {
        return $this->dropped;
    }

    public function partitioned(): bool
    {
        return $this->partitioning->by->count() > 0;
    }

    /**
     * @return Generator<array{Partitions, Rows}>
     */
    public function route(Rows $rows): Generator
    {
        if (!$this->partitioning->by->count()) {
            yield [new Partitions(), $rows];

            return;
        }

        $schema = $rows->schema();
        $this->assertSomethingIsLeftToWrite($schema);

        $stripped = $this->dropped === [] ? $schema : $schema->gracefulRemove(...$this->partitioning->by->names());

        foreach (KeyGrouping::indices((new KeyEncoder($this->partitioning->by->names()))->of($rows)) as $indices) {
            $group = $rows->gather($indices);

            yield [
                $this->rowPartitions->of($rows, $indices[0]),
                $this->dropped === [] ? $group : $group->select(...$stripped->names()),
            ];
        }
    }

    private function assertSomethingIsLeftToWrite(Schema $schema): void
    {
        if ($this->dropped === [] || $schema->count() > $this->partitioning->by->count()) {
            return;
        }

        throw new InvalidArgumentException(sprintf('No column left to write, every column is a partition column: "%s". '
        . 'Use partition_by(...)->writeColumns() to write them into the file as well.', implode(
            '", "',
            $this->partitioning->by->names(),
        )));
    }
}
