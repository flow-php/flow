<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;

final class LimitTransformer implements Limiting
{
    private int $rowsCount = 0;

    public function __construct(
        public readonly int $limit,
    ) {
        if ($this->limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: " . $this->limit);
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(type_integer()->assert($data['limit'] ?? null));
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['limit' => $this->limit]);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $this->rowsCount += $rows->count();

        return $this->rowsCount > $this->limit ? $rows->dropRight($this->rowsCount - $this->limit) : $rows;
    }

    public function reached(): bool
    {
        return $this->rowsCount >= $this->limit;
    }
}
