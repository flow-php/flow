<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Sort;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\SortOrder;
use SensitiveParameter;

use function constant;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;

final readonly class SortColumn implements Describable
{
    public function __construct(
        public string $name,
        public SortOrder $order = SortOrder::ASC,
        public NullsOrder $nulls = NullsOrder::FIRST,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            type_string()->assert($data['name'] ?? null),
            type_enum(SortOrder::class)->assert(constant(
                SortOrder::class . '::' . type_string()->assert($data['order'] ?? null),
            )),
            type_enum(NullsOrder::class)->assert(constant(
                NullsOrder::class . '::' . type_string()->assert($data['nulls'] ?? null),
            )),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'name' => $this->name,
            'order' => $this->order->name,
            'nulls' => $this->nulls->name,
        ]);
    }
}
