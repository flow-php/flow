<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Sheet;

use DOMElement;
use Generator;
use OpenSpout\Common\Exception\InvalidArgumentException;
use OpenSpout\Common\Exception\IOException;
use OpenSpout\Reader\Exception\XMLProcessingException;
use OpenSpout\Reader\XLSX\Helper\CellValueFormatter;
use OpenSpout\Reader\XLSX\RowIterator;
use OpenSpout\Reader\XLSX\Sheet;
use ReflectionProperty;
use XMLReader;

use function array_fill;
use function array_key_exists;
use function array_keys;
use function explode;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function ksort;
use function libxml_clear_errors;
use function libxml_get_last_error;
use function libxml_use_internal_errors;
use function max;
use function ord;
use function preg_match;
use function realpath;
use function rtrim;
use function sprintf;
use function strlen;
use function strspn;
use function trim;

/**
 * One XLSX sheet's rows, every cell formatted by OpenSpout's own CellValueFormatter - only the walk over the sheet
 * XML is Flow's. OpenSpout's RowIterator sends each XML node through a reflection callback and toggles libxml's
 * error mode around every read, which costs more than formatting the cells.
 *
 * @import-type SheetCellValue from SheetCells
 */
final readonly class XlsxSheetRows
{
    private const string LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

    private string $file;

    private CellValueFormatter $formatter;

    private string $part;

    public function __construct(Sheet $sheet)
    {
        // OpenSpout resolves the sheet's XML part and builds its formatter (shared strings, styles, the 1904 flag)
        // while it opens the workbook, and keeps both on the RowIterator alone: openspout is pinned to 5.3.x for
        // these three private properties
        $rows = $sheet->getRowIterator();

        $this->file = type_string()->assert((new ReflectionProperty(RowIterator::class, 'filePath'))->getValue($rows));
        $this->part = type_string()->assert((new ReflectionProperty(
            RowIterator::class,
            'sheetDataXMLFilePath',
        ))->getValue($rows));
        $this->formatter = type_instance_of(CellValueFormatter::class)->assert((new ReflectionProperty(
            RowIterator::class,
            'cellValueFormatter',
        ))->getValue($rows));
    }

    /**
     * Each non-empty row's cell values by column index, as OpenSpout's RowIterator yields them with the default
     * options Flow opens every workbook with: a row is padded to its spans, else to the sheet's dimension, and a
     * sheet that declares no dimension gets the gaps below its cell count filled.
     *
     * @return Generator<int, array<int, SheetCellValue>>
     */
    public function values(): Generator
    {
        $reader = new XMLReader();
        // libxml reports into its own buffer only while this generator runs - never across a yield
        $internalErrors = libxml_use_internal_errors(true);
        libxml_clear_errors();

        try {
            $file = realpath($this->file);

            // the PHP warning of a failed open repeats what the exception says
            if ($file === false || !@$reader->open('zip://' . $file . '#' . $this->part, null, LIBXML_NONET)) {
                throw new IOException(sprintf('Could not open "%s".', $this->part));
            }

            $dimension = 0;
            $values = [];
            $column = -1;
            $moved = $reader->read();

            while ($moved) {
                if ($reader->nodeType === XMLReader::ELEMENT && $reader->localName === 'c') {
                    $reference = $reader->getAttribute('r');
                    $column = $reference === null ? $column + 1 : self::column($reference);
                    // its PHP warning repeats the libxml error failure() reports
                    $node = @$reader->expand();

                    if (!$node instanceof DOMElement) {
                        throw self::failure();
                    }

                    $values[$column] = $this->formatter->extractAndFormatNodeValue($node)->getValue();
                    $moved = $reader->next();

                    continue;
                }

                if ($reader->nodeType === XMLReader::ELEMENT && $reader->localName === 'row') {
                    $spans = $reader->getAttribute('spans');
                    $width = $spans !== null && $spans !== '' ? (int) (explode(':', $spans)[1] ?? 0) : $dimension;
                    $values = $width > 0 ? array_fill(0, $width, '') : [];
                    $column = -1;
                } elseif ($reader->nodeType === XMLReader::ELEMENT && $reader->localName === 'dimension') {
                    $match = [];

                    if (preg_match('/[A-Z]+\d+:([A-Z]+\d+)/', (string) $reader->getAttribute('ref'), $match) === 1) {
                        $dimension = self::column($match[1]) + 1;
                    }
                } elseif ($reader->nodeType === XMLReader::END_ELEMENT && $reader->localName === 'row') {
                    if (libxml_get_last_error() !== false) {
                        throw self::failure();
                    }

                    if (!self::isEmpty($values)) {
                        libxml_use_internal_errors($internalErrors);

                        yield $dimension === 0 ? self::filled($values) : $values;

                        $internalErrors = libxml_use_internal_errors(true);
                        libxml_clear_errors();
                    }
                }

                $moved = $reader->read();
            }

            if (libxml_get_last_error() !== false) {
                throw self::failure();
            }
        } finally {
            $reader->close();
            libxml_use_internal_errors($internalErrors);
        }
    }

    /**
     * OpenSpout's CellHelper::getColumnIndexFromCellIndex(), without the two regular expressions it runs per cell.
     */
    private static function column(string $reference): int
    {
        $letters = rtrim($reference, '0123456789');
        $length = strlen($letters);

        if (
            $length < 1
            || $length > 3
            || strspn($letters, self::LETTERS) !== $length
            || $length === strlen($reference)
        ) {
            throw new InvalidArgumentException('Cannot get column index from an invalid cell index.');
        }

        $index = 0;

        for ($i = 0; $i < $length; $i++) {
            $index = ($index * 26) + ord($letters[$i]) - 64;
        }

        return $index - 1;
    }

    private static function failure(): XMLProcessingException
    {
        $error = libxml_get_last_error();

        return new XMLProcessingException($error === false ? 'The sheet XML could not be read' : trim($error->message));
    }

    /**
     * RowManager::fillMissingIndexesWithEmptyCells(): every gap below the highest column index, which is what
     * Row::getNumCells() counts.
     *
     * @param array<int, SheetCellValue> $values
     *
     * @return array<int, SheetCellValue>
     */
    private static function filled(array $values): array
    {
        $count = $values === [] ? 0 : max(array_keys($values)) + 1;
        $gaps = false;

        for ($index = 0; $index < $count; $index++) {
            if (!array_key_exists($index, $values)) {
                $values[$index] = '';
                $gaps = true;
            }
        }

        if ($gaps) {
            ksort($values);
        }

        return $values;
    }

    /**
     * Row::isEmpty(): only an EmptyCell holds '' or null.
     *
     * @param array<int, SheetCellValue> $values
     */
    private static function isEmpty(array $values): bool
    {
        foreach ($values as $value) {
            if ($value !== '' && $value !== null) {
                return false;
            }
        }

        return true;
    }
}
