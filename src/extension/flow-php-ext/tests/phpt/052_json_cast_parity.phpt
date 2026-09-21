--TEST--
native JSON cast matches PhpRowHydrator on the JSON parity cases and a deterministic fuzz corpus
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;

$cases = json_parity_cases();

$alphabet = ['[', ']', '{', '}', '"', ',', ':', '\\', 'u', 'd', '8', '0', 'D', 'C', 'e', 'E', '.', '-', '+', '1', '9', ' ', "\t", "\n", 't', 'r', 'u', 'e', 'n', 'l', 'f', "\x00", "\x1f", "\xff", "\xed", "\xa0", "\x80", "\xc3", "\xa9", "\xf4", "\x90", '\\u', '\\ud800', '\\udc00', '"a"', 'true', 'null', '[[[[', ']]]]'];
$seeds = array_values($cases);
mt_srand(52);

for ($i = 0; $i < 20000; $i++) {
    if ($i % 2) {
        $fuzzed = '';

        for ($length = mt_rand(1, 24), $j = 0; $j < $length; $j++) {
            $fuzzed .= $alphabet[mt_rand(0, count($alphabet) - 1)];
        }
    } else {
        $fuzzed = $seeds[mt_rand(0, count($seeds) - 1)];

        for ($mutations = mt_rand(1, 3), $j = 0; $j < $mutations; $j++) {
            $at = mt_rand(0, strlen($fuzzed));
            $operation = mt_rand(0, 2);
            $insert = $alphabet[mt_rand(0, count($alphabet) - 1)];
            $fuzzed = match ($operation) {
                0 => substr($fuzzed, 0, $at) . $insert . substr($fuzzed, $at),
                1 => substr($fuzzed, 0, $at) . substr($fuzzed, $at + 1),
                default => substr($fuzzed, 0, $at) . $insert . substr($fuzzed, $at + 1),
            };
        }
    }

    $cases["fuzz_{$i}"] = $fuzzed;
}

$schema = schema(json_schema('j', nullable: false));
$php = new PhpRowHydrator();
$native = new NativeRowHydrator();

$outcome = static function (Flow\ETL\Row\Hydrator $hydrator, string $value) use ($schema): string {
    try {
        return 'json ' . $hydrator->hydrate([new RawRowValues(['j' => $value])], $schema)->first()->get('j')->toString();
    } catch (Throwable $e) {
        return $e::class . ': ' . $e->getMessage();
    }
};

$mismatches = 0;

foreach ($cases as $name => $value) {
    if ($outcome($php, $value) !== $outcome($native, $value)) {
        $mismatches++;
        echo '  ', $name, ' ', bin2hex(substr($value, 0, 60)), "\n";
    }
}

printf("json cast parity: %d cases, %d mismatches\n", count($cases), $mismatches);
?>
--EXPECT--
json cast parity: 20054 cases, 0 mismatches
