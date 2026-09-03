--TEST--
native cast propagates the exact PHP cast exceptions and aborts the batch
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_string;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;

$throwing = [
    'integer from a non numeric string' => [schema(int_schema('id')), [new RawRowValues(['id' => 'abc'])]],
    'float from a hex string' => [schema(float_schema('p')), [new RawRowValues(['p' => '0x1A'])]],
    'boolean from an unrecognised word' => [schema(bool_schema('a')), [new RawRowValues(['a' => 'weird'])]],
    'integer from an array' => [schema(int_schema('id')), [new RawRowValues(['id' => [1, 2, 3]])]],
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
integer from a non numeric string exception:match aborted:yes
float from a hex string     exception:match aborted:yes
boolean from an unrecognised word exception:match aborted:yes
integer from an array       exception:match aborted:yes
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
