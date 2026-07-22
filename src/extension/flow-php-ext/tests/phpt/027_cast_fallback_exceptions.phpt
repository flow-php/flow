--TEST--
native cast propagates the exact PHP cast exceptions and aborts the batch
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;

use function Flow\ETL\DSL\{schema, datetime_schema, date_schema, uuid_schema, json_schema, list_schema, map_schema, structure_schema};
use function Flow\Types\DSL\{type_list, type_map, type_structure, type_integer, type_string, type_positive_integer};

$throwing = [
    'uuid invalid' => [schema(uuid_schema('u')), [new RawRowValues(['u' => 'not-a-uuid'])]],
    'uuid uppercase' => [schema(uuid_schema('u')), [new RawRowValues(['u' => '01234567-89AB-4DEF-8123-456789ABCDEF'])]],
    'json scalar' => [schema(json_schema('j')), [new RawRowValues(['j' => 5])]],
    'json invalid string' => [schema(json_schema('j')), [new RawRowValues(['j' => '{oops'])]],
    'json plain string' => [schema(json_schema('j')), [new RawRowValues(['j' => 'plain'])]],
    'datetime garbage' => [schema(datetime_schema('at')), [new RawRowValues(['at' => 'not-a-date'])]],
    'datetime array' => [schema(datetime_schema('at')), [new RawRowValues(['at' => ['nope']])]],
    'date garbage' => [schema(date_schema('d')), [new RawRowValues(['d' => 'not-a-date'])]],
    'string map int keys' => [
        schema(map_schema('m', type_map(type_string(), type_integer()))),
        [new RawRowValues(['m' => [5 => 1]])],
    ],
    'list bad keys' => [
        schema(list_schema('l', type_list(type_integer()))),
        [new RawRowValues(['l' => [1 => 'x']])],
    ],
    'positive int list string' => [
        schema(list_schema('l', type_list(type_positive_integer()))),
        [new RawRowValues(['l' => ['abc']])],
    ],
    'positive int list negative' => [
        schema(list_schema('l', type_list(type_positive_integer()))),
        [new RawRowValues(['l' => [-3]])],
    ],
    'all-optional structure' => [
        schema(structure_schema('st', type_structure([], ['b' => type_string()]))),
        [new RawRowValues(['st' => ['other' => 1]])],
    ],
];

$php = new PhpRowHydrator();
$native = new NativeRowHydrator();

foreach ($throwing as $label => [$s, $batch]) {
    $phpException = null;

    try {
        $php->cast($batch, $s);
    } catch (Throwable $e) {
        $phpException = $e::class . '|' . $e->getMessage();
    }

    $nativeException = null;
    $nativeResult = null;

    try {
        $nativeResult = $native->cast($batch, $s);
    } catch (Throwable $e) {
        $nativeException = $e::class . '|' . $e->getMessage();
    }

    printf(
        "%-27s exception:%s aborted:%s\n",
        $label,
        $phpException !== null && $phpException === $nativeException ? 'match' : "MISMATCH php[{$phpException}] native[{$nativeException}]",
        $nativeResult === null ? 'yes' : 'NO',
    );
}
?>
--EXPECT--
uuid invalid                exception:match aborted:yes
uuid uppercase              exception:match aborted:yes
json scalar                 exception:match aborted:yes
json invalid string         exception:match aborted:yes
json plain string           exception:match aborted:yes
datetime garbage            exception:match aborted:yes
datetime array              exception:match aborted:yes
date garbage                exception:match aborted:yes
string map int keys         exception:match aborted:yes
list bad keys               exception:match aborted:yes
positive int list string    exception:match aborted:yes
positive int list negative  exception:match aborted:yes
all-optional structure      exception:match aborted:yes
