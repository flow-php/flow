<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\DefinitionMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_shift;
use function explode;
use function preg_split;
use function str_contains;
use function str_starts_with;
use function trim;

final class MergeMatrixTest extends FlowTestCase
{
    /**
     * Rows are the receiver, columns the argument.
     */
    public static function provideMergeCases(): Generator
    {
        $grid = <<<'GRID'
                     null     bool     int      float    string   date     datetime time     uuid     enum     json     struct   list     map      union    html     htmlel   xml      xmlel
            null     null     bool     int      float    string   date     datetime time     uuid     enum     json     struct   list     map      union    html     htmlel   xml      xmlel
            bool     bool     bool     string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   string
            int      int      string   int      float    string   string   string   string   string   string   string   string   string   string   union    string   string   string   string
            float    float    string   float    float    string   string   string   string   string   string   string   string   string   string   string   string   string   string   string
            string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   union    string   string   string   string
            date     date     string   string   string   string   date     datetime datetime string   string   string   string   string   string   string   string   string   string   string
            datetime datetime string   string   string   string   datetime datetime datetime string   string   string   string   string   string   string   string   string   string   string
            time     time     string   string   string   string   datetime datetime time     string   string   string   string   string   string   string   string   string   string   string
            uuid     uuid     string   string   string   string   string   string   string   uuid     string   string   string   string   string   string   string   string   string   string
            enum     enum     string   string   string   string   string   string   string   string   enum     string   string   string   string   string   string   string   string   string
            json     json     string   string   string   string   string   string   string   string   string   json     json     json     json     string   string   string   string   string
            struct   struct   string   string   string   string   string   string   string   string   string   json     struct   json     json     string   string   string   string   string
            list     list     string   string   string   string   string   string   string   string   string   json     json     list     json     string   string   string   string   string
            map      map      string   string   string   string   string   string   string   string   string   json     json     json     map      string   string   string   string   string
            union    union    string   union    string   union    string   string   string   string   string   string   string   string   string   union    string   string   string   string
            html     html     string   string   string   string   string   string   string   string   string   string   string   string   string   string   html     string   string   string
            htmlel   htmlel   string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   htmlel   string   string
            xml      xml      string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   xml      string
            xmlel    xmlel    string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   string   xmlel
            GRID;

        $lines = explode("\n", $grid);
        $columns = preg_split('/\s+/', trim(array_shift($lines))) ?: [];

        foreach ($lines as $line) {
            $cells = preg_split('/\s+/', trim($line)) ?: [];
            $left = array_shift($cells);

            foreach ($cells as $index => $expected) {
                yield "{$left} merged with {$columns[$index]}" => [$left, $columns[$index], $expected];
            }
        }
    }

    /**
     * @param Definition<mixed> $definition
     */
    public static function columnName(Definition $definition): string
    {
        $type = $definition->type()->toString();

        return match (true) {
            str_starts_with($type, 'structure') => 'struct',
            str_starts_with($type, 'list') => 'list',
            str_starts_with($type, 'map') => 'map',
            str_starts_with($type, 'enum') => 'enum',
            str_contains($type, '|') => 'union',
            $type === 'boolean' => 'bool',
            $type === 'integer' => 'int',
            $type === 'html_element' => 'htmlel',
            $type === 'xml_element' => 'xmlel',
            default => $type,
        };
    }

    #[DataProvider('provideMergeCases')]
    public function test_merge(string $left, string $right, string $expected): void
    {
        $definitions = DefinitionMother::oneOfEachType();

        static::assertSame(
            $expected,
            self::columnName($definitions[$left]->merge($definitions[$right])),
            "{$left}->merge({$right})",
        );
    }

    #[DataProvider('provideMergeCases')]
    public function test_merge_is_symmetric(string $left, string $right, string $expected): void
    {
        $definitions = DefinitionMother::oneOfEachType();

        static::assertSame(
            $expected,
            self::columnName($definitions[$right]->merge($definitions[$left])),
            "{$right}->merge({$left}) disagrees with {$left}->merge({$right})",
        );
    }
}
