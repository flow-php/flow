--TEST--
The native JSON writer escapes a string as json_encode() does: random valid and invalid UTF-8, every combination of the four flags
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\JSON\PhpJsonEncoder;
use Flow\ETL\Adapter\JSON\RustJsonEncoder;

use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

mt_srand(78);

$codePoint = static fn(): int => match (mt_rand(0, 9)) {
    0 => mt_rand(0, 0x1f),
    1, 2, 3 => mt_rand(0x20, 0x7f),
    4 => [0x22, 0x5c, 0x2f, 0x3c, 0x3e, 0x26, 0x27][mt_rand(0, 6)],
    5 => mt_rand(0x80, 0x7ff),
    6 => mt_rand(0x800, 0xd7ff),
    7 => [0x2027, 0x2028, 0x2029, 0x202a, 0xfffd, 0xfffe, 0xffff, 0xe000][mt_rand(0, 7)],
    8 => mt_rand(0x10000, 0x10ffff),
    9 => [0x10000, 0x10ffff, 0x1f600][mt_rand(0, 2)],
};
$strings = ['', 'plain', '/', '"', '\\', "\u{2028}\u{2029}", "\xff", "a\xc3\x28", "\xed\xa0\x80", "\xc0\xaf", "\xf4\x90\x80\x80", "abc\xe2\x82", "\xf0\x9f\x98"];

for ($i = 0; $i < 1_500; $i++) {
    $text = '';

    for ($j = mt_rand(0, 12); $j > 0; $j--) {
        $text .= mb_chr($codePoint(), 'UTF-8');
    }

    $strings[] = $text;
}

for ($i = 0; $i < 500; $i++) {
    $strings[] = random_bytes_seeded(mt_rand(1, 8));
}

function random_bytes_seeded(int $length): string
{
    $bytes = '';

    for ($i = 0; $i < $length; $i++) {
        $bytes .= chr(mt_rand(0, 255));
    }

    return $bytes;
}

$schema = schema(str_schema('s'));
$php = array_map(static fn(string $text): Flow\ETL\Rows => php_rows($schema, [['s' => $text]]), $strings);
$native = array_map(static fn(string $text): Flow\ETL\Rows => native_rows($schema, [['s' => $text]]), $strings);
$encoded = static function (callable $encode): string {
    try {
        return $encode();
    } catch (Throwable $e) {
        return $e::class . ': ' . $e->getMessage();
    }
};

for ($combination = 0; $combination < 16; $combination++) {
    $flags = ($combination & 1 ? JSON_THROW_ON_ERROR : 0)
        | ($combination & 2 ? JSON_UNESCAPED_SLASHES : 0)
        | ($combination & 4 ? JSON_UNESCAPED_UNICODE : 0)
        | ($combination & 8 ? JSON_PRESERVE_ZERO_FRACTION : 0);
    $encoder = new PhpJsonEncoder($flags);
    $writer = new RustJsonEncoder($flags, DATE_ATOM, 'Y-m-d', $encoder);
    $different = 0;
    $refused = 0;

    foreach ($strings as $i => $text) {
        $expected = $encoded(static fn(): string => $encoder->encode($php[$i], "\n"));
        $refused += (int) refused($expected);

        if ($expected !== $encoded(static fn(): string => $writer->encode($native[$i], "\n")) && ++$different <= 3) {
            echo bin2hex($text), ': ', $expected, "\n";
        }
    }

    printf("flags %7d: %d of %d differ, %d refused\n", $flags, $different, count($strings), $refused);
}
?>
--EXPECT--
flags       0: 0 of 2013 differ, 442 refused
flags 4194304: 0 of 2013 differ, 442 refused
flags      64: 0 of 2013 differ, 442 refused
flags 4194368: 0 of 2013 differ, 442 refused
flags     256: 0 of 2013 differ, 442 refused
flags 4194560: 0 of 2013 differ, 442 refused
flags     320: 0 of 2013 differ, 442 refused
flags 4194624: 0 of 2013 differ, 442 refused
flags    1024: 0 of 2013 differ, 442 refused
flags 4195328: 0 of 2013 differ, 442 refused
flags    1088: 0 of 2013 differ, 442 refused
flags 4195392: 0 of 2013 differ, 442 refused
flags    1280: 0 of 2013 differ, 442 refused
flags 4195584: 0 of 2013 differ, 442 refused
flags    1344: 0 of 2013 differ, 442 refused
flags 4195648: 0 of 2013 differ, 442 refused
