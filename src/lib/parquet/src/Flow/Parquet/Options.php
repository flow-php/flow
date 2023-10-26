<?php declare(strict_types=1);

namespace Flow\Parquet;

final class Options
{
    /**
     * @var array<string, bool|int>
     */
    private array $options;

    public function __construct()
    {
        $this->options = [
            Option::BYTE_ARRAY_TO_STRING->name => true,
            Option::ROUND_NANOSECONDS->name => false,
            Option::INT_96_AS_DATETIME->name => true,
            Option::PAGE_SIZE_BYTES->name => 1024 * 8,
            Option::ROW_GROUP_SIZE_BYTES->name => 1024 * 1024 * 128,
        ];
    }

    public static function default() : self
    {
        return new self;
    }

    public function get(Option $option) : bool|int
    {
        return $this->options[$option->name];
    }

    public function set(Option $option, bool|int $value = true) : self
    {
        $this->options[$option->name] = $value;

        return $this;
    }
}
