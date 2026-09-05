<?php

declare(strict_types=1);

/**
 * Regenerates the workbooks the inference tests read. Run it from the repository root:
 *
 *     php src/adapter/etl-adapter-excel/tests/Flow/ETL/Adapter/Excel/Tests/Fixtures/generate.php
 *
 * The binaries are committed; this file exists so their content is readable and reproducible.
 * It does NOT touch the fixtures that predate schema inference (fixture.*, nullable_fixture.*,
 * orders_*.*, partitioned/, cross_stream/, empty_file, fixture_as_*).
 */

use OpenSpout\Common\Entity\Cell;
use OpenSpout\Common\Entity\Row;
use OpenSpout\Common\Entity\Style\Style;
use OpenSpout\Writer\ODS\Writer as OdsWriter;
use OpenSpout\Writer\WriterInterface;
use OpenSpout\Writer\XLSX\Writer as XlsxWriter;

require __DIR__ . '/../../../../../../../../../../vendor/autoload.php';

$dir = __DIR__;

/**
 * @param list<list<Cell|scalar|null>> $rows
 */
function write(WriterInterface $writer, string $file, array $rows): void
{
    $writer->openToFile($file);

    foreach ($rows as $cells) {
        $writer->addRow(new Row(array_map(static fn(mixed $cell): Cell => $cell instanceof Cell
            ? $cell
            : Cell::fromValue($cell), $cells)));
    }

    $writer->close();
}

/**
 * @param list<list<Cell|scalar|null>> $rows
 */
function write_xlsx(string $file, array $rows): void
{
    write(new XlsxWriter(), $file, $rows);
}

/**
 * @param list<list<Cell|scalar|null>> $rows
 */
function write_ods(string $file, array $rows): void
{
    write(new OdsWriter(), $file, $rows);
}

$dateStyle = (new Style())->withFormat('yyyy-mm-dd');
$dateTimeStyle = (new Style())->withFormat('yyyy-mm-dd hh:mm:ss');

write_xlsx($dir . '/header_only.xlsx', [['id', 'name']]);
write_ods($dir . '/header_only.ods', [['id', 'name']]);

write_xlsx($dir . '/empty_sheet.xlsx', []);
write_ods($dir . '/empty_sheet.ods', []);

write_xlsx($dir . '/dates_only.xlsx', [
    ['d'],
    [Cell::fromValue(new DateTimeImmutable('2024-01-01'), $dateStyle)],
    [Cell::fromValue(new DateTimeImmutable('2024-01-02'), $dateStyle)],
]);

write_xlsx($dir . '/dates_mixed.xlsx', [
    ['d'],
    [Cell::fromValue(new DateTimeImmutable('2024-01-01'), $dateStyle)],
    [Cell::fromValue(new DateTimeImmutable('2024-01-02'), $dateStyle)],
    [Cell::fromValue(new DateTimeImmutable('2024-01-03 10:30:00'), $dateTimeStyle)],
]);

write_xlsx($dir . '/int_then_str.xlsx', [['v'], [1], [2], ['n/a']]);

write_xlsx($dir . '/int_float_bool.xlsx', [
    ['i', 'f', 'b', 's'],
    [1, 1.5, true, 'x'],
    [2, 2.5, false, 'y'],
]);

@mkdir($dir . '/diverging_header');
write_xlsx($dir . '/diverging_header/a.xlsx', [['id', 'name'], [1, 'one']]);
write_xlsx($dir . '/diverging_header/b.xlsx', [['id', 'name', 'zip'], [2, 'two', '00-001']]);

// mixed_formats and sniff are copies of fixture.*: one glob spanning two OpenSpout readers, and
// extension-less paths, whose every open costs exactly one Filesystem::readFrom() to sniff the signature.
@mkdir($dir . '/mixed_formats');
copy($dir . '/fixture.xlsx', $dir . '/mixed_formats/a.xlsx');
copy($dir . '/fixture.ods', $dir . '/mixed_formats/b.ods');

// a listing whose FIRST workbook resolves no names at all: the header must come from the second
@mkdir($dir . '/empty_first');
copy($dir . '/empty_sheet.xlsx', $dir . '/empty_first/a.xlsx');
copy($dir . '/fixture.xlsx', $dir . '/empty_first/b.xlsx');

@mkdir($dir . '/sniff');
copy($dir . '/fixture.xlsx', $dir . '/sniff/a');
copy($dir . '/fixture.xlsx', $dir . '/sniff/b');

// a ZIP local-file-header signature with nothing behind it: ZipArchive::open() returns an error code
file_put_contents($dir . '/corrupt_zip', "\x50\x4B\x03\x04not a zip");

// the legacy XLS compound-file signature, which detect() hands to the XLSX reader
file_put_contents($dir . '/xls_signature', "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1and nothing else");

echo "written\n";
