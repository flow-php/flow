<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path;

use function Flow\Types\DSL\type_string;
use Symfony\Component\OptionsResolver\Exception\MissingOptionsException;

final class Options
{
    /**
     * @var array<string, mixed>
     */
    private array $options;

    /**
     * @param array<array-key, mixed> $options
     */
    public function __construct(array $options)
    {
        $normalizedOptions = [];

        foreach ($options as $option => $value) {
            $normalizedOptions[\mb_strtolower(type_string()->cast($option))] = $value;
        }

        $this->options = $normalizedOptions;
    }

    public function assertHas(string $option) : void
    {
        if (!$this->has($option)) {
            throw new MissingOptionsException("Option '{$option}' is missing in Path object.");
        }
    }

    public function get(string $option, ?string $default = null) : mixed
    {
        if ($this->has($option)) {
            return $this->options[$option];
        }

        return $default;
    }

    public function getAsString(string $option, ?string $default = null) : ?string
    {
        if ($this->has($option)) {
            return type_string()->cast($this->options[$option]);
        }

        return $default;
    }

    public function has(string $option) : bool
    {
        return isset($this->options[\mb_strtolower($option)]);
    }

    public function set(string $option, mixed $value) : void
    {
        $this->options[$option] = $value;
    }

    public function setWhenEmpty(string $option, mixed $value) : void
    {
        if (!$this->has($option)) {
            $this->options[$option] = $value;
        }
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray() : array
    {
        return $this->options;
    }
}
