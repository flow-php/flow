<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use DateTimeInterface;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\RoundTripPrecision;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\Types\Type;
use JsonException;

use function array_combine;
use function array_fill;
use function array_map;
use function count;
use function implode;
use function json_encode;
use function json_last_error_msg;

use const JSON_THROW_ON_ERROR;

final class PhpJSONEncoder implements JSONEncoder
{
    private readonly TextValues $text;

    public function __construct(
        private readonly int $flags = JSON_THROW_ON_ERROR,
        string $dateTimeFormat = DateTimeInterface::ATOM,
        string $dateFormat = 'Y-m-d',
    ) {
        $this->text = new TextValues($dateTimeFormat, $dateFormat);
    }

    public function encode(Rows $rows, string $separator): string
    {
        $count = $rows->count();

        if ($count < 1) {
            return '';
        }

        $names = [];
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $names[] = $definition->entry()->name();
            $columns[] = $this->text->of($definition->type(), $rows->column($definition->entry()->name())->physicals());
        }

        if ($columns === []) {
            $values = array_fill(0, $count, []);
        } elseif (count($columns) === 1) {
            $values = array_map(static fn(mixed $value): array => [$value], $columns[0]);
        } else {
            $values = array_map(null, ...$columns);
        }

        $documents = [];
        $precision = new RoundTripPrecision();
        $precision->force();

        try {
            foreach ($values as $row) {
                $documents[] = $this->json((object) array_combine($names, $row));
            }
        } finally {
            $precision->restore();
        }

        return implode($separator, $documents);
    }

    /**
     * @param Type<mixed> $type
     *
     * @return list<string> the JSON text of every cell
     */
    public function fragments(Type $type, Column $column): array
    {
        $fragments = [];
        $precision = new RoundTripPrecision();
        $precision->force();

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->text->of($type, $column->physicals()) as $value) {
                $fragments[] = $this->json($value);
            }
        } finally {
            $precision->restore();
        }

        return $fragments;
    }

    private function json(mixed $value): string
    {
        try {
            $json = json_encode($value, $this->flags);
        } catch (JsonException $e) {
            throw new RuntimeException('Failed to encode JSON: ' . $e->getMessage(), 0, $e);
        }

        if ($json === false) {
            throw new RuntimeException('Failed to encode JSON: ' . json_last_error_msg());
        }

        return $json;
    }
}
