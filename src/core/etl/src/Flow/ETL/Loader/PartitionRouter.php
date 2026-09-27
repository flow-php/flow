<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Bucketing\Hasher;
use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Partitions;
use Generator;

use function array_flip;
use function array_keys;
use function implode;
use function sprintf;

final class PartitionRouter
{
    private readonly RowPartitions $rowPartitions;

    /**
     * @var array<string, array-key>
     */
    private readonly array $dropped;

    public function __construct(
        private readonly Partitioning $partitioning,
        private readonly Hasher $hasher = new NativeHasher(),
    ) {
        $this->rowPartitions = new RowPartitions($partitioning->by);
        $this->dropped = $partitioning->writeColumns ? [] : array_flip($partitioning->by->names());
    }

    /**
     * @return array<string>
     */
    public function droppedNames(): array
    {
        return array_keys($this->dropped);
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

        $hashes = $this->hasher->hash((new KeyValues($this->partitioning->by->all()))->of($rows));

        /** @var array<string, array{Partitions, list<int>}> $groups */
        $groups = [];

        foreach ($hashes as $index => $hash) {
            $groups[$hash] ??= [$this->rowPartitions->of($rows, $index), []];
            $groups[$hash][1][] = $index;
        }

        $stripped = $this->dropped === [] ? $schema : $schema->gracefulRemove(...$this->partitioning->by->names());

        foreach ($groups as [$partitions, $indices]) {
            $group = $rows->gather($indices);

            yield [$partitions, $this->dropped === [] ? $group : $group->project($stripped)];
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
