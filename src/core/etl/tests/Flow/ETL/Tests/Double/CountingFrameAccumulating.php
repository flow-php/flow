<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\FrameAccumulating;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Window;
use Flow\ETL\Functions\Window\FrameAccumulation;
use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;

final class CountingFrameAccumulating implements FrameAccumulating, WindowFunction
{
    public int $accumulatorCalls = 0;

    public int $fedRows = 0;

    public function __construct(
        private readonly Reference $ref,
        private readonly ?Window $window = null,
        private readonly ?self $countersOwner = null,
    ) {}

    public function accumulator(): CountingAggregate
    {
        $this->counters()->accumulatorCalls++;

        return new CountingAggregate($this->ref, $this->counters());
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function values(Rows $partition, Backend $backend): array
    {
        return (new FrameAccumulation($this->window()->frame()))->values($this, $partition, $backend);
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return [$this->ref];
    }

    public function deterministic(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->deterministic()) {
                return false;
            }
        }

        return true;
    }

    public function resolved(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->resolved()) {
                return false;
            }
        }

        return true;
    }

    public function counters(): self
    {
        return $this->countersOwner ?? $this;
    }

    public function over(Window $window): static
    {
        return new self($this->ref, $window, $this->counters());
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_float());
    }

    public function toString(): string
    {
        return 'counting_frame_accumulating()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<Reference> $children */
        return new self($children[0], $this->window, $this->counters());
    }
}
