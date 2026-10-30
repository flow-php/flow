--TEST--
The native numeric-string grammar never accepts what PHP's is_numeric() refuses and yields PHP's values: 50 000 seeded grammar-directed strings with byte mutations plus the edge corpus, as int and float columns through RustColumnBuilder; a native refusal falls back to the PHP cast, so under-acceptance is invisible here and pinned by the crate's own tests
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;

use function Flow\ETL\DSL\{float_schema, int_schema};

mt_srand(101);

$spaces = [" ", "\t", "\n", "\r", "\v", "\f"];
$mutations = array_merge(str_split("0123456789 .eE+-\t\n\r\v\f\0x_"), ["\xA0"]);
$digits = static function (int $count): string {
    $digits = '';

    for ($i = 0; $i < $count; $i++) {
        $digits .= mt_rand(0, 9);
    }

    return $digits;
};
$generated = static function () use ($spaces, $mutations, $digits): string {
    $string = '';

    for ($i = mt_rand(0, 2); $i > 0; $i--) {
        $string .= $spaces[mt_rand(0, 5)];
    }

    if (mt_rand(0, 2) === 0) {
        $string .= mt_rand(0, 1) ? '-' : '+';
    }

    $string .= str_repeat('0', mt_rand(0, 3)) . $digits(mt_rand(0, 24));

    if (mt_rand(0, 1)) {
        $string .= '.' . $digits(mt_rand(0, 24));
    }

    if (mt_rand(0, 1)) {
        $string .= (mt_rand(0, 1) ? 'e' : 'E') . (mt_rand(0, 2) === 0 ? (mt_rand(0, 1) ? '-' : '+') : '') . $digits(mt_rand(0, 24));
    }

    for ($i = mt_rand(0, 2); $i > 0; $i--) {
        $string .= $spaces[mt_rand(0, 5)];
    }

    for ($i = mt_rand(0, 2); $i > 0; $i--) {
        $at = mt_rand(0, strlen($string));
        $string = substr($string, 0, $at) . $mutations[array_rand($mutations)] . substr($string, $at + mt_rand(0, 1));
    }

    return $string;
};

$strings = [
    '', ' ', '+', '-', '0000000000000000001', '00009223372036854775808', '9223372036854775807', '9223372036854775808',
    '9223372036854775809', '-9223372036854775808', '-9223372036854775808 ', '-9223372036854775809', '1.', '.5', '-.5',
    '+.5e-3', '1.e5', '.', '.e1', '1e', '1e+', '1E-5', '1e5.5', '1.5.5', '1e309', '1e-400', '4.9406564584124654e-324',
    '9007199254740993', '2.2250738585072011e-308', '1' . str_repeat('0', 800), '-0', '-0.0', '-0e0', "\v-0", "\f-0.0",
    "\f1", "1\f", "1\x002", "\x001", "1\x00", "\xA01", 'inf', 'nan', 'INF', '-INF', '0x1A', '1_000',
];

while (count($strings) < 50_000) {
    $strings[] = $generated();
}

$build = static fn(object $backend, object $definition, string $value): Closure => static function () use ($backend, $definition, $value): array {
    $builder = $backend->builder($definition);
    $builder->append($value);

    return $builder->finish()->values();
};

$identical = 0;
$numeric = 0;
$values = 0;

foreach ($strings as $string) {
    $numeric += (int) is_numeric($string);

    foreach ([[int_schema('i'), (int) $string], [float_schema('f'), (float) $string]] as [$definition, $cast]) {
        $php = outcome($build(new PhpBackend(), $definition, $string));
        $native = outcome($build(new RustBackend(), $definition, $string));

        if ($php !== $native) {
            echo $definition->type()->toString(), ' ', json_encode($string), "\n  php:    {$php}\n  native: {$native}\n";

            continue;
        }

        $identical++;

        // (int) and (float) of a named float ('INF') or a leading-numeric string differ from the typed casts by design
        if (refused($native) || !is_numeric($string)) {
            continue;
        }

        $values++;

        if ($native !== comparable([$cast])) {
            echo $definition->type()->toString(), ' ', json_encode($string), "\n  (type) cast: ", comparable([$cast]), "\n  native:      {$native}\n";
        }
    }
}

printf("%d strings, %d numeric: %d of %d outcomes identical, %d numeric values equal to PHP's (int) / (float) cast\n", count($strings), $numeric, $identical, 2 * count($strings), $values);
?>
--EXPECT--
50000 strings, 27035 numeric: 100000 of 100000 outcomes identical, 39043 numeric values equal to PHP's (int) / (float) cast
