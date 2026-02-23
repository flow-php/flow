<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Flow\Types\Value\Json;
use Twig\Extension\AbstractExtension;
use Twig\TwigFilter;

final class FlowExtension extends AbstractExtension
{
    /**
     * Formats a base64-encoded doc comment into HTML.
     * Strips /** and * / markers, removes leading asterisks, converts newlines to <br>.
     */
    public function formatDocComment(?string $docComment) : string
    {
        if ($docComment === null || $docComment === '') {
            return '';
        }

        $decoded = \base64_decode($docComment, true);

        if ($decoded === false) {
            return '';
        }

        $stripped = \preg_replace('/^\/\*\*|\*\/$/', '', $decoded);
        $lines = \explode("\n", (string) $stripped);
        $lines = \array_map(static fn (string $line) : string => (string) \preg_replace('/^\s*\*\s?/', '', $line), $lines);
        $lines = \array_filter($lines, static fn (string $line) : bool => \trim($line) !== '');

        return \implode('<br>', $lines);
    }

    /**
     * Formats an array of type objects into a pipe-separated string.
     *
     * @param array<array{name: string}>|Json|string $types
     */
    public function formatType(array|Json|string $types) : string
    {
        $types = $this->toArray($types);

        return \implode('|', \array_map(static fn (array $t) : string => $t['name'], $types));
    }

    #[\Override]
    public function getFilters() : array
    {
        return [
            new TwigFilter('strpad', $this->strpad(...), ['is_safe' => ['html']]),
            new TwigFilter('format_doc_comment', $this->formatDocComment(...), ['is_safe' => ['html']]),
            new TwigFilter('format_type', $this->formatType(...), ['is_safe' => ['html']]),
            new TwigFilter('to_array', $this->toArray(...)),
        ];
    }

    public function strpad(string|int|float $input, int $length, string $padString = '', string|int $padType = 'left') : string
    {
        if (!\is_string($input)) {
            $input = (string) $input;
        }

        if (\is_string($padType)) {
            $padType = match (true) {
                \stristr($padType, 'left') !== false => STR_PAD_LEFT,
                \stristr($padType, 'both') !== false => STR_PAD_BOTH,
                default => STR_PAD_RIGHT,
            };
        }

        return \str_pad($input, $length, $padString, $padType);
    }

    /**
     * Converts Json or string to array.
     *
     * @param array<mixed>|Json|string $value
     *
     * @return array<mixed>
     */
    public function toArray(array|Json|string $value) : array
    {
        if ($value instanceof Json) {
            return $value->toArray();
        }

        if (\is_string($value)) {
            $decoded = \json_decode($value, true);

            return \is_array($decoded) ? $decoded : [];
        }

        return $value;
    }
}
