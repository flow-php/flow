<?php declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\Exception\InvalidArgumentException;

final class Options
{
    /**
     * @var array<string, bool|float|int>
     */
    private array $options;

    public function __construct()
    {
        $this->options = [
            Option::BYTE_ARRAY_TO_STRING->name => true,
            Option::ROUND_NANOSECONDS->name => false,
            Option::INT_96_AS_DATETIME->name => true,
            Option::PAGE_SIZE_BYTES->name => Consts::KB_SIZE * 8,
            Option::ROW_GROUP_SIZE_BYTES->name => Consts::MB_SIZE * 128,
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL->name => 1000,
            Option::DICTIONARY_PAGE_SIZE->name => Consts::MB_SIZE,
            Option::DICTIONARY_PAGE_MIN_CARDINALITY_RATION->name => 0.4,
            Option::GZIP_COMPRESSION_LEVEL->name => 9,
            Option::WRITER_VERSION->name => 1,
            Option::VALIDATE_DATA->name => true,
        ];
    }

    public static function default() : self
    {
        return new self;
    }

    public function get(Option $option) : bool|int|float
    {
        return $this->options[$option->name];
    }

    public function getBool(Option $option) : bool
    {
        $value = $this->options[$option->name];

        if (!\is_bool($value)) {
            throw new InvalidArgumentException("Option {$option->name} is not a boolean, but: " . \gettype($value));
        }

        return $value;
    }

    public function getInt(Option $option) : int
    {
        $value = $this->options[$option->name];

        if (!\is_int($value)) {
            throw new InvalidArgumentException("Option {$option->name} is not an integer, but: " . \gettype($value));
        }

        return $value;
    }

    public function set(Option $option, bool|int|float $value) : self
    {
        $this->options[$option->name] = $value;

        return $this;
    }
}
