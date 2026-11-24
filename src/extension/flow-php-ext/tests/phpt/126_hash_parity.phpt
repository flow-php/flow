--TEST--
RustBackend::hash() answers as PhpBackend::hash() does for every HashAlgorithm: values, null count and type, over a Rust and a PHP string column with empty, multibyte, 1 MB, raw byte and null values, and hash(ref()) through a data frame on RustBackend gives the pinned digests
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Values\Column\{Backend, Column, PhpBackend, RustBackend};

use function Flow\ETL\DSL\{array_to_rows, bool_schema, config_builder, data_frame, datetime_schema, float_schema, from_array, int_schema, json_schema, list_schema, ref, schema, str_schema, to_memory};
use function Flow\Types\DSL\{type_list, type_string};
use function Flow\ETL\DSL\hash as hash_of;

$php = new PhpBackend();
$rust = new RustBackend();
$values = ['test', '', 'zażółć', str_repeat('ab', 512 * 1024), "\x00\xff", null];
$column = static fn(Backend $backend): Column => array_to_rows(
    array_map(static fn(?string $value): array => ['s' => $value], $values),
    schema(str_schema('s', nullable: true)),
    $backend,
)->column('s');
$described = static fn(Column $column): string => json_encode([$column->values(), $column->nullCount(), $column->type()->toString()]);

foreach (HashAlgorithm::cases() as $algorithm) {
    $expected = $described($php->hash($column($php), $algorithm));
    $results = [];

    foreach (['rust column' => $column($rust), 'php column' => $column($php)] as $label => $strings) {
        $actual = $described($rust->hash($strings, $algorithm));
        $results[] = $label . ' ' . ($actual === $expected ? 'same' : "MISMATCH {$actual} {$expected}");
    }

    echo $algorithm->value, ': ', implode(', ', $results), "\n";
}

$kinds = [
    'string' => [str_schema('v', nullable: true), ['test', '', 'zażółć', null], [
        'xxh128' => ['6c78e0e3bd51d358d01e758642b85fb8', '99aa06d3014798d86001c324468d497f', 'bdd8ac7ac90b11bb31701c8e12e9349d', null],
        'md5' => ['098f6bcd4621d373cade4e832627b4f6', 'd41d8cd98f00b204e9800998ecf8427e', '884c73f2b5319ab2bbda84aacb14ec68', null],
        'sha256' => [
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
            'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
            '7eefe5e3046c86cec19ce1215e0a04724124d948468005a1ebc4adf2643bea13',
            null,
        ],
    ]],
    'integer' => [int_schema('v'), [1], ['xxh128' => ['df3ce784d856334d65cd25028f98f158'], 'md5' => ['c4ca4238a0b923820dcc509a6f75849b'], 'sha256' => ['6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b']]],
    'float' => [float_schema('v'), [1.5], ['xxh128' => ['72e27827e1082b043d006affbf7f50ee'], 'md5' => ['6008647277c4454cecd97d33c069f0ca'], 'sha256' => ['9f29a130438b81170b92a42650f9a94291ecad60bd47af2a3886e75f7f728725']]],
    'true' => [bool_schema('v'), [true], ['xxh128' => ['df3ce784d856334d65cd25028f98f158'], 'md5' => ['c4ca4238a0b923820dcc509a6f75849b'], 'sha256' => ['6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b']]],
    'false' => [bool_schema('v'), [false], ['xxh128' => ['99aa06d3014798d86001c324468d497f'], 'md5' => ['d41d8cd98f00b204e9800998ecf8427e'], 'sha256' => ['e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855']]],
    'list' => [list_schema('v', type_list(type_string())), [['test']], ['xxh128' => ['4450cf82dc53848e2bbe9798b70b0a6a'], 'md5' => ['acc75d777f16492f95ba7c572335b7f7'], 'sha256' => ['18f4cf4cf6ad53618cd47d604ef6a6f9c0cbc6755d6caa31f333e851b3671328']]],
    'datetime' => [datetime_schema('v'), [new DateTimeImmutable('2021-01-01')], ['xxh128' => ['5347d10de38eb5570c044eb710a5120a'], 'md5' => ['7a0362b9779aebdc1d5899683970157f'], 'sha256' => ['c7db551aff37ffed2f35e6a5b8449499bef83a5036f84847f03f9c9eedadb05e']]],
    'json' => [json_schema('v'), ['{"a":1}'], ['xxh128' => ['1922c6c4ef7a35d299a4567fb5a80d47'], 'md5' => ['38bd6ceec415d6d69c5d5f29bbfbf480'], 'sha256' => ['506c2c44e41105954056801d93014e5603eb2b3acaa4bfee9e07eb0a5e072ff0']]],
];

foreach ($kinds as $kind => [$definition, $inputs, $digests]) {
    $results = [];

    foreach (HashAlgorithm::cases() as $algorithm) {
        $memory = new ArrayMemory();
        data_frame(config_builder()->backend($rust))
            ->read(from_array(array_map(static fn(mixed $input): array => ['v' => $input], $inputs), schema($definition)))
            ->withColumn('h', hash_of(ref('v'), $algorithm))
            ->write(to_memory($memory))
            ->run();

        $actual = array_column($memory->dump(), 'h');
        $results[] = $algorithm->value . ' ' . ($actual === $digests[$algorithm->value] ? 'pinned' : 'MISMATCH ' . json_encode($actual));
    }

    echo $kind, ' data frame: ', implode(', ', $results), "\n";
}
?>
--EXPECT--
md5: rust column same, php column same
sha256: rust column same, php column same
xxh128: rust column same, php column same
string data frame: md5 pinned, sha256 pinned, xxh128 pinned
integer data frame: md5 pinned, sha256 pinned, xxh128 pinned
float data frame: md5 pinned, sha256 pinned, xxh128 pinned
true data frame: md5 pinned, sha256 pinned, xxh128 pinned
false data frame: md5 pinned, sha256 pinned, xxh128 pinned
list data frame: md5 pinned, sha256 pinned, xxh128 pinned
datetime data frame: md5 pinned, sha256 pinned, xxh128 pinned
json data frame: md5 pinned, sha256 pinned, xxh128 pinned
