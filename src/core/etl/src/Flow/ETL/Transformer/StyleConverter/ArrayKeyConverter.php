<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\StyleConverter;

/**
 * @psalm-immutable
 */
final class ArrayKeyConverter
{
    /**
     * @psalm-var pure-callable(string) : string
     *
     * @var callable(string) : string
     */
    private $transformer;

    /**
     * @psalm-param pure-callable(string) : string $transformer
     *
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
