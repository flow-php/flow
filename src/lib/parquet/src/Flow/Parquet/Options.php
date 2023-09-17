<?php declare(strict_types=1);

namespace Flow\Parquet;

final class Options
{
    /**
     * @var array<string, bool>
     */
    private array $options;

    public function __construct()
    {
        $this->options = [
            Option::BYTE_ARRAY_TO_STRING->name => false,
            Option::ROUND_NANOSECONDS->name => false,
            Option::INT_96_AS_DATETIME->name => true,
        ];
    }

    public function get(Option $option) : bool
    {
        return $this->options[$option->name];
    }

    public function set(Option $option, bool $value = true) : self
    {
        $this->options[$option->name] = $value;

        return $this;
    }
}
