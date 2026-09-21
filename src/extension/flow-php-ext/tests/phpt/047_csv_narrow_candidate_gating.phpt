--TEST--
native narrowing skips exactly the rungs a restricted candidate set leaves out
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Schema\Inference\InferredTypes;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;

$corpus = csv_narrow_corpus();

foreach ([
    'all strings' => new InferredTypes(type_string()),
    'integer' => new InferredTypes(type_integer()),
    'float integer' => new InferredTypes(type_float(), type_integer()),
    'date' => new InferredTypes(type_date()),
    'datetime' => new InferredTypes(type_datetime()),
    'boolean timezone' => new InferredTypes(type_boolean(), type_time_zone()),
    'json uuid' => new InferredTypes(type_json(), type_uuid()),
] as $label => $candidates) {
    assert_narrow_parity($label, $candidates->toArray(), $corpus);
}
?>
--EXPECT--
all strings: identical
integer: identical
float integer: identical
date: identical
datetime: identical
boolean timezone: identical
json uuid: identical
