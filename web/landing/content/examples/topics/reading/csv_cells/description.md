Header names are trimmed, and a blank header cell is named `e` + its position (`e01`). A short line is padded with
`null` under `withMissingColumnsAsNull(true)`, else it is a corrupted record; `empty_to_null` only decides what a present
empty cell becomes.
