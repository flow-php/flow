<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\IO\Loader\RowPartitions;
use Flow\ETL\Runtime\Metrics\DatasetHash;
use Flow\ETL\Runtime\Metrics\Hash;
use Flow\ETL\Runtime\Metrics\PartitionHash;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\GroupedCounts;
use Flow\ETL\Values\Column\GroupedMultisetHashes;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use Flow\Types\Type\Native\FloatType;
use SensitiveParameter;

use function array_map;
use function array_unique;
use function Flow\Types\DSL\type_string;
use function sprintf;

final class PartitionedOutputHasher implements OutputHasher
{
    private ?GroupTable $table = null;

    private ?GroupedMultisetHashes $digests = null;

    private ?GroupedCounts $rows = null;

    private Schema $declared;

    private readonly RowPartitions $rowPartitions;

    /**
     * @var list<Partitions> by group id
     */
    private array $partitions = [];

    private DatasetHash $absorbed;

    public function __construct(
        private readonly References $by,
        private readonly HashAlgorithm $algorithm,
    ) {
        $this->absorbed = new DatasetHash(Hash::empty(), [], $algorithm);
        $this->declared = new Schema();
        $this->rowPartitions = new RowPartitions($by);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['by'] ?? null, References::class),
            HashAlgorithm::named(type_string()->assert($data['algorithm'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['by' => $this->by, 'algorithm' => $this->algorithm->value]);
    }

    public function add(Rows $rows, Backend $backend): void
    {
        if ($rows->isEmpty()) {
            return;
        }

        if ($this->table === null) {
            $this->assertPartitionable($rows);
            $this->declared = $rows->schema()->keep(...array_unique([
                ...array_map(static fn(Reference $ref): string => $ref->base(), $this->by->all()),
                ...$this->by->names(),
            ]));
            $this->table = $backend->groupTable($this->by->names(), $rows->schema());
        }

        foreach ($this->by as $ref) {
            if (
                $rows->schema()->findDefinition($ref->base()) === null
                && !$this->declared->get($ref->base())->isNullable()
            ) {
                throw self::missing($ref->base());
            }
        }

        $ids = $this->table->assign($rows);
        ($this->digests ??= $backend->groupedMultisetHashes($this->algorithm))->add($rows->columns(), $ids->ids);
        ($this->rows ??= $backend->groupedCounts())->add($ids->ids, $ids->ids);

        if ($ids->firstRows === []) {
            return;
        }

        $firsts = $rows->gather($ids->firstRows)->castTo($this->declared, $backend);

        for ($index = 0; $index < $firsts->count(); $index++) {
            $this->partitions[] = $this->rowPartitions->of($firsts, $index);
        }
    }

    public function absorb(DatasetHash $partial): void
    {
        $this->absorbed = $this->absorbed->plus($partial);
    }

    public function hash(): DatasetHash
    {
        $hashes = $this->digests?->hashes()->physicals() ?? [];
        $counts = $this->rows?->partials() ?? [];
        $local = [];
        $total = Hash::empty();

        foreach ($this->partitions as $id => $partitions) {
            $local[] = new PartitionHash($partitions, new Hash($counts[$id], type_string()->assert($hashes[$id])));
            $total = $total->plus($local[$id]->hash);
        }

        return $this->absorbed->plus(new DatasetHash($total, $local, $this->algorithm));
    }

    private static function missing(string $column): InvalidArgumentException
    {
        return new InvalidArgumentException(sprintf(
            'analyze()->withDatasetHash() groups by "%s", but a batch has no column "%s". Name only columns that '
            . 'every row of the output has.',
            $column,
            $column,
        ));
    }

    private function assertPartitionable(Rows $rows): void
    {
        foreach ($this->by as $ref) {
            $definition = $rows->schema()->findDefinition($ref->base()) ?? throw self::missing($ref->base());

            if ($definition->type() instanceof FloatType) {
                throw new InvalidArgumentException(sprintf(
                    'analyze()->withDatasetHash() cannot group by the float column "%s": 0.0 and -0.0 are one value '
                    . 'but two partition names. Cast it to a decimal string or an integer first.',
                    $ref->base(),
                ));
            }

            Partition::assertPartitionable($ref->base(), $definition->type());
        }
    }
}
