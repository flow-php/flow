<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\ValueConverter;

use function is_array;

final readonly class ElementsValueConverter implements ValueConverter
{
    public function __construct(
        private ValueConverter $element,
    ) {}

    public function decode(mixed $value): mixed
    {
        if (!is_array($value)) {
            return $value;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $element) {
            if ($element !== null) {
                $value[$key] = $this->element->decode($element);
            }
        }

        return $value;
    }

    public function encode(mixed $value): mixed
    {
        if (!is_array($value)) {
            return $value;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $element) {
            if ($element !== null) {
                $value[$key] = $this->element->encode($element);
            }
        }

        return $value;
    }
}
