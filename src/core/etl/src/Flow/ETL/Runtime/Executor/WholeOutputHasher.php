<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Runtime\Metrics\DatasetHash;
use Flow\ETL\Runtime\Metrics\Hash;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\GroupedMultisetHashes;
use Flow\ETL\Values\Column\MultisetDigest;
use Flow\ETL\Values\Column\MultisetHashAlgorithm;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema\Definitions;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class WholeOutputHasher implements OutputHasher
{
    private ?GroupedMultisetHashes $hasher = null;

    private int $rows = 0;

    private DatasetHash $absorbed;

    public function __construct(
        private readonly MultisetHashAlgorithm $algorithm,
    ) {
        $this->absorbed = new DatasetHash(Hash::empty(), [], $algorithm);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(MultisetHashAlgorithm::named(type_string()->assert($data['algorithm'] ?? null)));
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['algorithm' => $this->algorithm->value]);
    }

    public function add(Rows $rows, Backend $backend): void
    {
        $this->rows += $rows->count();
        ($this->hasher ??= $backend->groupedMultisetHashes($this->algorithm))->add($rows->columns(), $backend->constant(
            Definitions::of('group', type_integer()),
            0,
            $rows->count(),
        ));
    }

    public function absorb(DatasetHash $partial): void
    {
        $this->absorbed = $this->absorbed->plus(new DatasetHash($partial->total, [], $partial->algorithm));
    }

    public function hash(): DatasetHash
    {
        return (new DatasetHash(
            new Hash(
                $this->rows,
                type_string()->assert($this->hasher?->hashes()->physicals()[0] ?? MultisetDigest::empty()->hex()),
            ),
            [],
            $this->algorithm,
        ))->plus($this->absorbed);
    }
}
