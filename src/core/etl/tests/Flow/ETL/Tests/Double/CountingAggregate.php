<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Sum;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function count;

final readonly class CountingAggregate implements AggregatingFunction
{
    private Sum $sum;

    public function __construct(
        Reference $ref,
        private CountingFrameAccumulating $spy,
    ) {
        $this->sum = new Sum($ref);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        $this->spy->fedRows += count($indices);
        $this->sum->aggregate($rows, $indices, $backend);
    }

    public function children(): array
    {
        return $this->sum->children();
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function deterministic(): bool
    {
        return $this->sum->deterministic();
    }

    public function merge(array $partial, Backend $backend): void
    {
        $this->sum->merge($partial, $backend);
    }

    public function outputName(): string
    {
        return $this->sum->outputName();
    }

    public function partial(): array
    {
        return $this->sum->partial();
    }

    public function partialType(): StructureType
    {
        return $this->sum->partialType();
    }

    public function references(): ?array
    {
        return $this->sum->references();
    }

    public function resolved(): bool
    {
        return $this->sum->resolved();
    }

    public function returns(): Type
    {
        return $this->sum->returns();
    }

    public function value(): ?float
    {
        return $this->sum->value();
    }

    public function withChildren(array $children): static
    {
        throw InvalidArgumentException::because('%s is built by its spy, never rebuilt', self::class);
    }
}
