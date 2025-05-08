<?php

declare(strict_types=1);

namespace Flow\ETL\String;

use function Symfony\Component\String\u;
use Flow\ETL\Exception\InvalidArgumentException;
use Symfony\Component\String\Slugger\AsciiSlugger;
use Symfony\Component\String\UnicodeString;

enum StringStyles : string
{
    case ASCII = 'ascii';
    case CAMEL = 'camel';
    case KEBAB = 'kebab';
    case LOWER = 'lower';
    case SLUG = 'slug';
    case SNAKE = 'snake';
    case TITLE = 'title';
    case UCFIRST = 'ucfirst';
    case UCWORDS = 'ucwords';
    case UPPER = 'upper';

    /**
     * @return array<string>
     */
    public static function all() : array
    {
        $cases = [];

        foreach (self::cases() as $case) {
            $cases[] = $case->value;
        }

        return $cases;
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromString(string $style) : self
    {
        foreach (self::cases() as $case) {
            if ($style === $case->value) {
                return $case;
            }
        }

        throw new InvalidArgumentException("Unrecognized style {$style}, please use one of following: " . \implode(', ', self::all()));
    }

    public function convert(string $value) : string
    {
        return match ($this) {
            self::ASCII => u($value)->ascii()->toString(),
            self::CAMEL => u($value)->camel()->toString(),
            self::KEBAB => $this->kebab($value),
            self::LOWER => \mb_strtolower($value),
            self::SLUG => $this->slug($value),
            self::SNAKE => u($value)->snake()->toString(),
            self::TITLE => u($value)->title()->toString(),
            self::UCFIRST => $this->ucFirst($value),
            self::UCWORDS => $this->ucWords($value),
            self::UPPER => \mb_strtoupper($value),
        };
    }

    private function kebab(string $value) : string
    {
        // @phpstan-ignore-next-line Available from Symfony 7.2+
        if (!\method_exists(UnicodeString::class, 'kebab')) {
            return u($value)->snake()->replace('_', '-')->toString();
        }

        return u($value)->kebab()->toString();
    }

    private function slug(string $string) : string
    {
        return (new AsciiSlugger())->slug($string)->toString();
    }

    private function ucFirst(string $string) : string
    {
        // Available from PHP 8.4+
        if (\function_exists('mb_ucfirst')) {
            return \mb_ucfirst($string);
        }

        $encoding = \mb_internal_encoding();

        return \mb_strtoupper(\mb_substr($string, 0, 1, $encoding), $encoding) . \mb_substr($string, 1, null, $encoding);
    }

    private function ucWords(string $string) : string
    {
        $result = '';
        $previousCharacter = ' ';

        $encoding = \mb_internal_encoding();

        for ($i = 0, $length = \mb_strlen($string, $encoding); $i < $length; $i++) {
            $currentCharacter = \mb_substr($string, $i, 1, $encoding);

            if (' ' === $previousCharacter) {
                $currentCharacter = \mb_strtoupper($currentCharacter, $encoding);
            }

            $result .= $currentCharacter;
            $previousCharacter = $currentCharacter;
        }

        return $result;
    }
}
