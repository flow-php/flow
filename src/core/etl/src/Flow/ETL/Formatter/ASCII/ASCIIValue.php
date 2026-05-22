<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use JsonException;

use function floor;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function json_encode;
use function mb_strlen;
use function mb_substr;
use function str_repeat;
use function str_replace;

use const JSON_THROW_ON_ERROR;

final class ASCIIValue
{
    private ?string $stringValue = null;

    /**
     * @param null|array<mixed>|bool|Entry<mixed>|float|int|string $value
     */
    public function __construct(
        private readonly string|int|bool|float|array|Entry|null $value,
    ) {}

    /**
     * Solution and all credits goes to https://stackoverflow.com/a/58272671.
     *
     * @param string $input
     * @param int $length
     * @param string $padding
     * @param int $padType
     * @param string $encoding
     *
     * @return string
     */
    public static function mb_str_pad(
        string $input,
        int $length,
        string $padding = ' ',
        int $padType = STR_PAD_RIGHT,
        string $encoding = 'UTF-8',
    ): string {
        $result = $input;

        if (($paddingRequired = $length - mb_strlen($input, $encoding)) > 0) {
            switch ($padType) {
                case STR_PAD_LEFT:
                    return mb_substr(str_repeat($padding, $paddingRequired), 0, $paddingRequired, $encoding) . $input;
                case STR_PAD_RIGHT:
                    return $input . mb_substr(str_repeat($padding, $paddingRequired), 0, $paddingRequired, $encoding);
                case STR_PAD_BOTH:
                    $leftPaddingLength = (int) floor($paddingRequired / 2);
                    $rightPaddingLength = $paddingRequired - $leftPaddingLength;

                    return (
                        mb_substr(str_repeat($padding, $leftPaddingLength), 0, $leftPaddingLength, $encoding)
                        . $input
                        . mb_substr(str_repeat($padding, $rightPaddingLength), 0, $rightPaddingLength, $encoding)
                    );
            }
        }

        return $result;
    }

    public function length(int|bool $truncate = 20): int
    {
        return mb_strlen($this->print($truncate));
    }

    public function print(int|bool $truncate = 20): string
    {
        if ($truncate === 0) {
            $truncate = false;
        }

        if ($truncate === false) {
            return $this->stringValue();
        }

        if (is_int($truncate)) {
            if (mb_strlen($this->stringValue()) <= $truncate) {
                return $this->stringValue();
            }

            return mb_substr($this->stringValue(), 0, $truncate);
        }

        // $truncate = true - default 20
        if (mb_strlen($this->stringValue()) <= 20) {
            return $this->stringValue();
        }

        return mb_substr($this->stringValue(), 0, 20);
    }

    private function stringValue(): string
    {
        if ($this->stringValue === null) {
            try {
                $val = $this->value;

                if ($val instanceof Entry) {
                    $this->stringValue = $val->toString();

                    if ($val instanceof XMLEntry || $val instanceof XMLElementEntry) {
                        $this->stringValue = str_replace("\n", '', $this->stringValue);
                    }

                    return $this->stringValue;
                }

                if ($val === null) {
                    $this->stringValue = 'null';

                    return $this->stringValue;
                }

                $this->stringValue = match (true) {
                    is_string($val) => $val,
                    is_bool($val) => $val ? 'true' : 'false',
                    is_int($val), is_float($val) => (string) $val,
                    default => json_encode($val, JSON_THROW_ON_ERROR),
                };
            } catch (JsonException) {
                $this->stringValue = '{...}';
            }
        }

        return $this->stringValue;
    }
}
