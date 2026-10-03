<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use Flow\Types\Value\Json;

use function assert;
use function is_string;

final readonly class JsonPhysical implements Physical
{
    public function __construct(
        private JsonFactory $factory = new JsonFactory(),
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof Json);

        return $value->toString();
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        return $this->factory->create($physical);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_string($physical));
            $values[] = $physical === null ? null : $this->factory->create($physical);
        }

        return $values;
    }
}
