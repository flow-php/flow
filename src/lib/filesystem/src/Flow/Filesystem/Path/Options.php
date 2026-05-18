<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path;

use UnitEnum;

use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_scalar;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function mb_strtolower;

final readonly class Options
{
    /**
     * @var array<string, null|bool|float|int|string|\UnitEnum>
     */
    private array $options;

    /**
     * @param array<array-key, null|bool|float|int|string|\UnitEnum> $options
     */
    public function __construct(array $options)
    {
        $normalizedOptions = [];

        foreach ($options as $option => $value) {
            $normalizedOptions[mb_strtolower(type_string()->cast($option))] = type_union(
                type_scalar(),
                type_enum(UnitEnum::class),
            )->assert($value);
        }

        $this->options = $normalizedOptions;
    }

    public function get(
        string|Option $option,
        string|int|bool|float|UnitEnum|null $default = null,
    ): string|int|bool|float|UnitEnum|null {
        if ($this->has($option)) {
            return $this->options[$option instanceof Option ? $option->value : $option];
        }

        return $default;
    }

    public function has(string|Option $option): bool
    {
        return isset($this->options[mb_strtolower($option instanceof Option ? $option->value : $option)]);
    }

    public function set(string|Option $option, string|int|bool|float|UnitEnum|null $value): self
    {
        $newOptions = $this->options;
        $newOptions[$option instanceof Option ? $option->value : $option] = $value;

        return new self($newOptions);
    }

    public function setWhenEmpty(string|Option $option, string|int|bool|float|UnitEnum|null $value): self
    {
        if (!$this->has($option)) {
            return $this->set($option, $value);
        }

        return $this;
    }

    /**
     * @return array<string, null|bool|float|int|string|\UnitEnum>
     */
    public function toArray(): array
    {
        return $this->options;
    }
}
