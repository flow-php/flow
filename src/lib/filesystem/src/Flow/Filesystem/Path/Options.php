<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path;

final class Options
{
    /**
     * @var array<string, mixed>
     */
    private array $options;

    public function __construct(array $options)
    {
        $normalizedOptions = [];

        foreach ($options as $option => $value) {
            $normalizedOptions[\mb_strtolower((string) $option)] = $value;
        }

        $this->options = $normalizedOptions;
    }

    public function getAsString(string $option, ?string $default = null) : ?string
    {
        if ($this->has($option)) {
            return (string) $this->options[$option];
        }

        return $default;
    }

    public function has(string $option) : bool
    {
        return isset($this->options[\mb_strtolower($option)]);
    }

    public function toArray() : array
    {
        return $this->options;
    }
}
