<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Context;

use RuntimeException;
use ZipArchive;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function md5;

/**
 * fixture.xlsx with its first sheet's XML replaced. Its styles stay, so a crafted sheet may use any number format,
 * but no shared strings - inline strings and numbers only.
 */
final class CraftedWorkbook
{
    public static function with(string $sheetXml): string
    {
        $target = __DIR__ . '/../Integration/var/crafted_' . md5($sheetXml) . '.xlsx';
        $filesystem = native_local_filesystem();
        $filesystem
            ->writeTo(path($target))
            ->append($filesystem->readFrom(ExcelFixtureContext::path('fixture.xlsx'))->content())
            ->close();

        $zip = new ZipArchive();

        if ($zip->open($target) !== true) {
            throw new RuntimeException('Cannot open ' . $target);
        }

        $zip->addFromString('xl/worksheets/sheet1.xml', $sheetXml);
        $zip->close();

        return $target;
    }

    public static function sheet(string $rows, ?string $dimension = null): string
    {
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            . '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            . ($dimension === null ? '' : '<dimension ref="' . $dimension . '"/>')
            . '<sheetData>'
            . $rows
            . '</sheetData></worksheet>'
        );
    }
}
