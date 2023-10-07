<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression\StyleConverter;

final class ArrayKeyConverter
{
    /**
     * @var callable(string) : string
     */
    private $transformer;

    /**
     * @param callable(string) : string $transformer
     */
    public function __construct(callable $transformer)
    {
        $this->transformer = $transformer;
    }

    /**
     * @param array<mixed> $array
     *
     * @return array<mixed>
     */
    public function convert(array $array) : array
    {
        $newArray = [];

        /** @psalm-suppress MixedAssignment */
        foreach ($array as $key => $value) {
            $newKey = \is_string($key) ? ($this->transformer)($key) : $key;

            if (\is_array($value)) {
                $value = $this->convert($value);
            }

            $newArray[$newKey] = $value;
        }

        return $newArray;
    }
}
