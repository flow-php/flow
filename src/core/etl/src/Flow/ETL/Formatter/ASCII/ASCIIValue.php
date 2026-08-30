<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\Types\Exception\Exception as TypesException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\TypedValueFormatter;

use function floor;
use function is_int;
use function mb_strlen;
use function mb_substr;
use function str_repeat;
use function str_replace;

final class ASCIIValue
{
    private ?string $stringValue = null;

    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        private readonly Type $type,
        private readonly mixed $value,
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
            $leftPaddingLength = (int) floor($paddingRequired / 2);
            $rightPaddingLength = $paddingRequired - $leftPaddingLength;

            return match ($padType) {
                STR_PAD_LEFT => mb_substr(str_repeat($padding, $paddingRequired), 0, $paddingRequired, $encoding)
                    . $input,
                STR_PAD_RIGHT => $input
                    . mb_substr(str_repeat($padding, $paddingRequired), 0, $paddingRequired, $encoding),
                STR_PAD_BOTH => mb_substr(
                    str_repeat($padding, max(0, $leftPaddingLength)),
                    0,
                    $leftPaddingLength,
                    $encoding,
                )
                    . $input
                    . mb_substr(str_repeat($padding, max(0, $rightPaddingLength)), 0, $rightPaddingLength, $encoding),
                default => $result,
            };
        }

        return $result;
    }

    /**
     * @return int<0, max>
     */
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
                $this->stringValue = (new TypedValueFormatter())->format($this->type, $this->value);
            } catch (TypesException) {
                $this->stringValue = '{...}';

                return $this->stringValue;
            }

            if ($this->type instanceof XMLType || $this->type instanceof XMLElementType) {
                $this->stringValue = str_replace('
', '', $this->stringValue);
            }
        }

        return $this->stringValue;
    }
}
